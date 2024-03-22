/*
 * include/shard/FSTrie.h
 *
 * Copyright (C) 2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the FSTrie learned index.
 */
#pragma once


#include <vector>

#include "framework/ShardRequirements.h"
#include "fst.hpp"
#include "util/SortedMerge.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::byte;

namespace de {

template <KVPInterface R>
class FSTrie {
private:

    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    static_assert(std::is_same_v<K, std::string>, "FST requires std::string keys.");

public:
    FSTrie(BufferView<R> buffer)
        : m_data(nullptr)
        , m_reccnt(0)
        , m_alloc_size(0)
    {
        m_data = new Wrapped<R>[buffer.get_record_count()]();
        m_alloc_size = sizeof(Wrapped<R>) * buffer.get_record_count();

        size_t cnt = 0;
        std::vector<K> keys;
        keys.reserve(buffer.get_record_count());

        /*
         * Copy the contents of the buffer view into a temporary buffer, and
         * sort them. We still need to iterate over these temporary records to 
         * apply tombstone/deleted record filtering, as well as any possible
         * per-record processing that is required by the shard being built.
         */
        auto temp_buffer = new Wrapped<R>[buffer.get_record_count()]();
        for (size_t i=0; i<buffer.get_record_count(); i++) {
            temp_buffer[i] = *(buffer.get(i));
        }

        auto base = temp_buffer;
        auto stop = base + buffer.get_record_count();
        std::sort(base, stop, std::less<Wrapped<R>>());

        for (size_t i=0; i<buffer.get_record_count(); i++) {
            if (temp_buffer[i].is_deleted() || !temp_buffer[i].is_visible()) {
                continue;
            }

            m_data[cnt] = temp_buffer[i];
            m_data[cnt].clear_timestamp();

            keys.push_back(m_data[cnt].rec.key);
            cnt++;
        }

        for (size_t i=0; i<keys.size() - 1; i++) {
            assert(keys[i] <= keys[i+1]);
        }

        m_reccnt = cnt;
        if (m_reccnt > 0) {
            m_fst = new fst::Trie(keys);
        }

        delete[] temp_buffer;
    }

    FSTrie(std::vector<FSTrie*> &shards) 
        : m_data(nullptr)
        , m_reccnt(0)
        , m_alloc_size(0)
    {
        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        auto cursors = build_cursor_vec<R, FSTrie>(shards, &attemp_reccnt, &tombstone_count);
        
        m_data = new Wrapped<R>[attemp_reccnt]();
        m_alloc_size = attemp_reccnt * sizeof(Wrapped<R>);

        std::vector<K> keys;
        keys.reserve(attemp_reccnt);

        // FIXME: For smaller cursor arrays, it may be more efficient to skip
        //        the priority queue and just do a scan.
        PriorityQueue<Wrapped<R>> pq(cursors.size());
        for (size_t i=0; i<cursors.size(); i++) {
            pq.push(cursors[i].ptr, i);
        }

        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
            /* 
             * if the current record is not a tombstone, and the next record is
             * a tombstone that matches the current one, then the current one
             * has been deleted, and both it and its tombstone can be skipped
             * over.
             */
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                /* skip over records that have been deleted via tagging */
                if (!cursor.ptr->is_deleted() && cursor.ptr->rec.key != "") {
                    m_data[m_reccnt] = *cursor.ptr;
                    keys.push_back(m_data[m_reccnt].rec.key);

                    m_reccnt++;
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        for (size_t i=0; i<keys.size() - 1; i++) {
            assert(keys[i] <= keys[i+1]);
        }

        if (m_reccnt > 0) {
            m_fst = new fst::Trie(keys);
        }
    }

    ~FSTrie() {
        delete[] m_data;
        delete m_fst;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {

        auto idx = m_fst->exactSearch(rec.key);

        if (idx == fst::kNotFound) {
            return nullptr;
        }

        // FIXME: for convenience, I'm treating this Trie as a unique index
        // for now, so no need to scan forward and/or check values. This
        // also makes the point lookup query class a lot easier to make.
        // Ultimately, though, we can support non-unique indexes with some
        // extra work.

        return m_data + idx;
    }

    Wrapped<R>* get_data() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return 0;
    }

    const Wrapped<R>* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx;
    }


    size_t get_memory_usage() {
        return m_fst->getMemoryUsage() + m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        return 0;
    }

    size_t get_lower_bound(R &rec) {return 0;}
    size_t get_upper_bound(R &rec) {return 0;}

private:

    Wrapped<R>* m_data;
    size_t m_reccnt;
    size_t m_alloc_size;
    fst::Trie *m_fst;
};
}
