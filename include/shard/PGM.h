/*
 * include/shard/PGM.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the static version of the PGM learned
 * index.
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once


#include <vector>

#include "framework/ShardRequirements.h"

#include "pgm/pgm_index.hpp"
#include "psu-ds/BloomFilter.h"
#include "util/SortedMerge.h"
#include "util/bf_config.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::byte;

namespace de {

template <RecordInterface R, size_t epsilon=128>
class PGM {
public:
    typedef R RECORD;
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

public:
    PGM(BufferView<R> buffer)
        : m_bf(nullptr)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0) {

        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

        std::vector<K> keys;
        /*
         * Copy the contents of the buffer view into a temporary buffer, and
         * sort them. We still need to iterate over these temporary records to 
         * apply tombstone/deleted record filtering, as well as any possible
         * per-record processing that is required by the shard being built.
         */
        auto temp_buffer = (Wrapped<R> *) psudb::sf_aligned_calloc(CACHELINE_SIZE, 
                                                                   buffer.get_record_count(), 
                                                                   sizeof(Wrapped<R>));
        buffer.copy_to_buffer((byte *) temp_buffer);

        auto base = temp_buffer;
        auto stop = base + buffer.get_record_count();
        std::sort(base, stop, std::less<Wrapped<R>>());

        merge_info info = {0, 0};

        /* 
         * Iterate over the temporary buffer to process the records, copying
         * them into buffer as needed
         */
        while (base < stop) {
            if (!base->is_tombstone() && (base + 1 < stop)
                && base->rec == (base + 1)->rec  && (base + 1)->is_tombstone()) {
                base += 2;
                continue;
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            // FIXME: this shouldn't be necessary, but the tagged record
            // bypass doesn't seem to be working on this code-path, so this
            // ensures that tagged records from the buffer are able to be
            // dropped, eventually. It should only need to be &= 1
            base->header &= 3;
            keys.emplace_back(base->rec.key);
            m_data[info.record_count++] = *base;

            if (base->is_tombstone()) {
                info.tombstone_count++;
                if (m_bf){
                    m_bf->insert(base->rec);
                }
            }

            base++;
        }

        free(temp_buffer);

        m_reccnt = info.record_count;
        m_tombstone_cnt = info.tombstone_count;

        if (m_reccnt > 0) {
            m_pgm = pgm::PGMIndex<K, epsilon>(keys);
        }
    }

    PGM(std::vector<PGM*> const &shards)
        : m_data(nullptr)
        , m_bf(nullptr)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0) {
        
        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        auto cursors = build_cursor_vec<R, PGM>(shards, &attemp_reccnt, &tombstone_count);

        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);
        std::vector<K> keys;

        // FIXME: For smaller cursor arrays, it may be more efficient to skip
        //        the priority queue and just do a scan.
        PriorityQueue<Wrapped<R>> pq(cursors.size());
        for (size_t i=0; i<cursors.size(); i++) {
            pq.push(cursors[i].ptr, i);
        }

        merge_info info = {0, 0};
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
                    keys.emplace_back(cursor.ptr->rec.key);
                    m_data[info.record_count++] = *cursor.ptr;

                    /*  
                     * if the record is a tombstone, increment the ts count and 
                     * insert it into the bloom filter if one has been
                     * provided.
                     */
                    if (cursor.ptr->is_tombstone()) {
                        info.tombstone_count++;
                        if (m_bf) {
                            m_bf->insert(cursor.ptr->rec);
                        }
                    }
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        m_reccnt = info.record_count;
        m_tombstone_cnt = info.tombstone_count;

        if (m_reccnt > 0) {
            m_pgm = pgm::PGMIndex<K, epsilon>(keys);
        }
   }

    ~PGM() {
        free(m_data);
        delete m_bf;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        size_t idx = get_lower_bound(rec.key);
        if (idx >= m_reccnt) {
            return nullptr;
        }

        while (idx < m_reccnt && m_data[idx].rec < rec) ++idx;

        if (m_data[idx].rec == rec) {
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
        return m_tombstone_cnt;
    }

    const Wrapped<R>* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx;
    }


    size_t get_memory_usage() {
        return m_pgm.size_in_bytes();
    }

    size_t get_aux_memory_usage() {
        return (m_bf) ? m_bf->memory_usage() : 0;
    }

    size_t get_lower_bound(const K& key) const {
        auto bound = m_pgm.search(key);
        size_t idx = bound.lo;

        if (idx >= m_reccnt) {
            return m_reccnt;
        }

        /*
         * If the region to search is less than some pre-specified
         * amount, perform a linear scan to locate the record.
         */
        if (bound.hi - bound.lo < 256) {
            while (idx < bound.hi && m_data[idx].rec.key < key) {
                idx++;
            }
        } else {
            /* Otherwise, perform a binary search */
            idx = bound.lo;
            size_t max = bound.hi;

            while (idx < max) {
                size_t mid = (idx + max) / 2;
                if (key > m_data[mid].rec.key) {
                    idx = mid + 1;
                } else {
                    max = mid;
                }
            }

        }

        /*
         * the upper bound returned by PGM is one passed the end of the
         * array. If we are at that point, we should just return "not found"
         */
        if (idx == m_reccnt) {
            return idx;
        }

        /* 
         * We may have walked one passed the actual lower bound, so check
         * the index before the current one to see if it is the actual bound
         */
        if (m_data[idx].rec.key > key && idx > 0 && m_data[idx-1].rec.key <= key) {
            return idx-1;
        }

        /*
         * Otherwise, check idx. If it is a valid bound, then return it,
         * otherwise return "not found".
         */
        return (m_data[idx].rec.key >= key) ? idx : m_reccnt;
    }

private:
    Wrapped<R>* m_data;
    BloomFilter<R> *m_bf;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_alloc_size;
    K m_max_key;
    K m_min_key;
    pgm::PGMIndex<K, epsilon> m_pgm;
};

}
