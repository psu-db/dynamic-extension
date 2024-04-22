/*
 * include/shard/TrieSpline.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the TrieSpline learned index.
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once


#include <vector>

#include "framework/ShardRequirements.h"
#include "ts/builder.h"
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
class TrieSpline {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

public:
    TrieSpline(BufferView<R> buffer)
        : m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_max_key(0)
        , m_min_key(0)
        , m_bf(nullptr)
    {
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

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

        auto tmp_min_key = temp_buffer[0].rec.key;
        auto tmp_max_key = temp_buffer[buffer.get_record_count() - 1].rec.key;
        auto bldr = ts::Builder<K>(tmp_min_key, tmp_max_key, E);

        merge_info info = {0, 0};

        m_min_key = tmp_max_key;
        m_max_key = tmp_min_key;

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
            bldr.AddKey(base->rec.key);
            m_data[info.record_count++] = *base;

            if (base->is_tombstone()) {
                info.tombstone_count++;
                if (m_bf){
                    m_bf->insert(base->rec);
                }
            }

            if (base->rec.key < m_min_key) {
                m_min_key = base->rec.key;
            } 

            if (base->rec.key > m_max_key) {
                m_max_key = base->rec.key;
            }

            base++;
        }

        free(temp_buffer);

        m_reccnt = info.record_count;
        m_tombstone_cnt = info.tombstone_count;

        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
    }

    TrieSpline(std::vector<TrieSpline*> &shards) 
        : m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_max_key(0)
        , m_min_key(0)
        , m_bf(nullptr)
    {
        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        auto cursors = build_cursor_vec<R, TrieSpline>(shards, &attemp_reccnt, &tombstone_count);
        
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);

        // FIXME: For smaller cursor arrays, it may be more efficient to skip
        //        the priority queue and just do a scan.
        PriorityQueue<Wrapped<R>> pq(cursors.size());
        for (size_t i=0; i<cursors.size(); i++) {
            pq.push(cursors[i].ptr, i);
        }

        auto tmp_max_key = shards[0]->m_max_key;
        auto tmp_min_key = shards[0]->m_min_key;

        for (size_t i=0; i<shards.size(); i++) {
            if (shards[i]->m_max_key > tmp_max_key) {
                tmp_max_key = shards[i]->m_max_key;
            }

            if (shards[i]->m_min_key < tmp_min_key) {
                tmp_min_key = shards[i]->m_min_key;
            }
        }

        auto bldr = ts::Builder<K>(tmp_min_key, tmp_max_key, E);

        m_max_key = tmp_min_key;
        m_min_key = tmp_max_key;

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
                    bldr.AddKey(cursor.ptr->rec.key);
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

                    if (cursor.ptr->rec.key < m_min_key) {
                        m_min_key = cursor.ptr->rec.key;
                    }

                    if (cursor.ptr->rec.key > m_max_key) {
                        m_max_key = cursor.ptr->rec.key;
                    }
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        m_reccnt = info.record_count;
        m_tombstone_cnt = info.tombstone_count;

        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
    }

    ~TrieSpline() {
        free(m_data);
        delete m_bf;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if (filter && m_bf && !m_bf->lookup(rec)) {
            return nullptr;
        }

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
        return m_ts.GetSize();
    }

    size_t get_aux_memory_usage() {
        return (m_bf) ? m_bf->memory_usage() : 0;
    }

    size_t get_lower_bound(const K& key) const {
        auto bound = m_ts.GetSearchBound(key);
        size_t idx = bound.begin;

        if (idx >= m_reccnt) {
            return m_reccnt;
        }

        // If the region to search is less than some pre-specified
        // amount, perform a linear scan to locate the record.
        if (bound.end - bound.begin < 256) {
            while (idx < bound.end && m_data[idx].rec.key < key) {
                idx++;
            }
        } else {
            // Otherwise, perform a binary search
            idx = bound.begin;
            size_t max = bound.end;

            while (idx < max) {
                size_t mid = (idx + max) / 2;
                if (key > m_data[mid].rec.key) {
                    idx = mid + 1;
                } else {
                    max = mid;
                }
            }
        }

        if (idx == m_reccnt) {
            return m_reccnt;
        }

        if (m_data[idx].rec.key > key && idx > 0 && m_data[idx-1].rec.key <= key) {
            return idx-1;
        }

        return idx;
    }

private:

    Wrapped<R>* m_data;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_alloc_size;
    K m_max_key;
    K m_min_key;
    ts::TrieSpline<K> m_ts;
    BloomFilter<R> *m_bf;
};
}
