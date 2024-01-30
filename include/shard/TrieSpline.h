/*
 * include/shard/TrieSpline.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the TrieSpline learned index.
 *
 */
#pragma once


#include <vector>

#include "framework/ShardRequirements.h"
#include "ts/builder.h"
#include "psu-ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "psu-ds/BloomFilter.h"
#include "util/bf_config.h"
#include "psu-util/timer.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;

namespace de {

template <KVPInterface R, size_t E=1024>
class TrieSpline {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

public:
    TrieSpline(BufferView<R> buffer)
        : m_data(nullptr)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_max_key(0)
        , m_min_key(0)
        , m_bf(new BloomFilter<R>(BF_FPR, buffer.get_tombstone_count(), BF_HASH_FUNCS))
    {
        TIMER_INIT();

        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

        TIMER_START();
        auto temp_buffer = (Wrapped<R> *) psudb::sf_aligned_calloc(CACHELINE_SIZE, buffer.get_record_count(), sizeof(Wrapped<R>));
        buffer.copy_to_buffer((byte *) temp_buffer);

        auto base = temp_buffer;
        auto stop = base + buffer.get_record_count();
        std::sort(base, stop, std::less<Wrapped<R>>());

        K min_key = base->rec.key;
        K max_key = (stop-1)->rec.key;
        TIMER_STOP();

        auto sort_time = TIMER_RESULT();

        TIMER_START();
        auto bldr = ts::Builder<K>(min_key, max_key, E);
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
            m_data[m_reccnt++] = *base;
            bldr.AddKey(base->rec.key);
            if (m_bf && base->is_tombstone()) {
                ++m_tombstone_cnt;
                m_bf->insert(base->rec);
            }

            /*
             * determine the "true" min/max keys based on the scan. This is
             * to avoid situations where the min/max in the input array
             * are deleted and don't survive into the structure itself.
             */
            if (m_reccnt == 0) {
                m_max_key = m_min_key = base->rec.key;
            } else if (base->rec.key > m_max_key) {
                m_max_key = base->rec.key;
            } else if (base->rec.key < m_min_key) {
                m_min_key = base->rec.key;
            }

            base++;
        }

        TIMER_STOP();
        auto copy_time = TIMER_RESULT();

        TIMER_START();
        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
        TIMER_STOP();
        auto level_time = TIMER_RESULT();

        free(temp_buffer);
    }

    TrieSpline(std::vector<TrieSpline*> &shards) 
        : m_data(nullptr)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_max_key(0)
        , m_min_key(0)
        , m_bf(nullptr)
    {

        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(shards.size());

        PriorityQueue<Wrapped<R>> pq(shards.size());

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;

        /*
         * Initialize m_max_key and m_min_key using the values from the
         * first shard. These will later be updated when building
         * the initial priority queue to their true values.
         */
        m_max_key = shards[0]->m_max_key;
        m_min_key = shards[0]->m_min_key;
        
        for (size_t i = 0; i < shards.size(); ++i) {
            if (shards[i]) {
                auto base = shards[i]->get_data();
                cursors.emplace_back(Cursor{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
                attemp_reccnt += shards[i]->get_record_count();
                tombstone_count += shards[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);

                if (shards[i]->m_max_key > m_max_key) {
                    m_max_key = shards[i]->m_max_key;
                } 

                if (shards[i]->m_min_key < m_min_key) {
                    m_min_key = shards[i]->m_min_key;
                }
            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);

        auto bldr = ts::Builder<K>(m_min_key, m_max_key, E);
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    bldr.AddKey(cursor.ptr->rec.key);
                    if (cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        m_bf->insert(cursor.ptr->rec);
                    }
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
    }

    ~TrieSpline() {
        free(m_data);
        delete m_bf;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if (filter && !m_bf->lookup(rec)) {
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
        return m_ts.GetSize() + m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        return 0;
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
