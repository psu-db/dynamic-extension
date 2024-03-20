/*
 * include/shard/FSTrie.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the FSTrie learned index.
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once


#include <vector>

#include "framework/ShardRequirements.h"
#include "FST.hpp"
#include "psu-ds/BloomFilter.h"
#include "util/bf_config.h"
#include "util/SortedMerge.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::byte;

namespace de {

template <KVPInterface R, size_t E=1024>
class FSTrie {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

    static_assert(std::is_same_v<K, std::string> || std::is_same_v<K, uint64_t>, 
    "FST requires either string or uint64_t keys");

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

        std::vector<size_t> values;
        values.reserve(buffer.get_record_count());

        size_t longest_key = 0;

        /*
         * Copy the contents of the buffer view into a temporary buffer, and
         * sort them. We still need to iterate over these temporary records to 
         * apply tombstone/deleted record filtering, as well as any possible
         * per-record processing that is required by the shard being built.
         */
        /*
        auto temp_buffer = (Wrapped<R> *) psudb::sf_aligned_calloc(CACHELINE_SIZE, 
                                                                   buffer.get_record_count(), 
                                                                   sizeof(Wrapped<R>));
                                                                   */
        auto temp_buffer = new Wrapped<R>[buffer.get_record_count()]();
        for (size_t i=0; i<buffer.get_record_count(); i++) {
            temp_buffer[i] = *(buffer.get(i));
        }

        auto base = temp_buffer;
        auto stop = base + buffer.get_record_count();
        std::sort(base, stop, std::less<Wrapped<R>>());

        for (size_t i=0; i<buffer.get_record_count(); i++) {
            if (temp_buffer[i].is_deleted()) {
                continue;
            }

            m_data[cnt] = temp_buffer[i];
            m_data[cnt].header = 0;

            keys.push_back(m_data[cnt].rec.key);
            values.push_back(cnt);
            if constexpr (std::is_same_v<K, std::string>) {
                if (m_data[cnt].rec.key.size() > longest_key) {
                    longest_key = m_data[cnt].rec.key.size();
                }
            }

            cnt++;
        }

        for (size_t i=0; i<keys.size() - 1; i++) {
            assert(keys[i] <= keys[i+1]);
        }

        m_reccnt = cnt;
        m_fst = FST();
        if constexpr (std::is_same_v<K, std::string>) {
            m_fst.load(keys, values, longest_key);
        } else {
            m_fst.load(keys, values);
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

        std::vector<size_t> values;
        values.reserve(attemp_reccnt);

        size_t longest_key = 0;
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
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt] = *cursor.ptr;
                    keys.push_back(m_data[m_reccnt].rec.key);
                    values.push_back(m_data[m_reccnt].rec.value);

                    if constexpr (std::is_same_v<K, std::string>) {
                        if (m_data[m_reccnt].rec.key.size() > longest_key) {
                            longest_key = m_data[m_reccnt].rec.key.size();
                        }
                    }

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
            m_fst = FST();
            if constexpr (std::is_same_v<K, std::string>) {
                m_fst.load(keys, values, longest_key);
            } else {
                m_fst.load(keys, values);
            }
        }
    }

    ~FSTrie() {
        delete[] m_data;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        size_t idx;
        bool res; 
        if constexpr (std::is_same_v<K, std::string>) {
            res = m_fst.lookup((uint8_t*)rec.key.c_str(), rec.key.size(), idx);
        } else {
            res = m_fst.lookup(rec.key, idx);
        }

        if (res && m_data[idx].rec.key != rec.key) {
            fprintf(stderr, "ERROR!\n");
        }

        if (res) {
            return m_data + idx;
        }

        return nullptr;
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
        return m_fst.mem() + m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        return 0;
    }

    size_t get_lower_bound(const K& key) {
        auto itr = FSTIter();

        const K temp_key = key;

        bool res;
        if constexpr (std::is_same_v<K, std::string>) {
           res = m_fst.lowerBound(temp_key.c_str(), key.size(), itr); 
        } else {
           res = m_fst.lowerBound(temp_key, itr); 
        }

        return itr.value();
    }

    size_t get_upper_bound(const K& key) {
        auto itr = FSTIter();

        const K temp_key = key;

        bool res;
        if constexpr (std::is_same_v<K, std::string>) {
           res = m_fst.lowerBound(temp_key.c_str(), key.size(), itr); 
        } else {
           res = m_fst.lowerBound(temp_key, itr); 
        }

        size_t idx = itr.value();
        while (idx < m_reccnt && m_data[idx].rec.key <= key) {
            idx++;
        }

        return idx;
    }

private:

    Wrapped<R>* m_data;
    size_t m_reccnt;
    size_t m_alloc_size;
    FST m_fst;
};
}
