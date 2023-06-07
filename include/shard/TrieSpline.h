/*
 * include/shard/TrieSpline.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once


#include <vector>
#include <cassert>
#include <queue>
#include <memory>
#include <concepts>

#include "ts/builder.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/BloomFilter.h"
#include "util/bf_config.h"
#include "framework/MutableBuffer.h"
#include "framework/RecordInterface.h"
#include "framework/ShardInterface.h"
#include "framework/QueryInterface.h"

namespace de {

size_t g_max_error = 1024;

template <RecordInterface R>
struct ts_range_query_parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
};

template <RecordInterface R>
class TrieSplineRangeQuery;

template <RecordInterface R>
struct TrieSplineState {
    size_t start_idx;
    size_t stop_idx;
};

template <RecordInterface R>
struct TrieSplineBufferState {
    size_t cutoff;
    Alias* alias;

    ~TrieSplineBufferState() {
        delete alias;
    }

};

template <RecordInterface R>
class TrieSpline {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

public:

    // FIXME: there has to be a better way to do this
    friend class TrieSplineRangeQuery<R>;

    TrieSpline(MutableBuffer<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0) {

        size_t alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        m_bf = new BloomFilter<K>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        std::sort(base, stop, std::less<Wrapped<R>>());

        K min_key = base->rec.key;
        K max_key = (stop - 1)->rec.key;

        auto bldr = ts::Builder<K>(min_key, max_key, g_max_error);

        while (base < stop) {
            if (!(base->is_tombstone()) && (base + 1) < stop) {
                if (base->rec == (base + 1)->rec && (base + 1)->is_tombstone()) {
                    base += 2;
                    continue;
                }
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            if (m_reccnt == 0) {
                m_max_key = m_min_key = base->rec.key;
            } else if (base->rec.key > m_max_key) {
                m_max_key = base->rec.key;
            } else if (base->rec.key < m_min_key) {
                m_min_key = base->rec.key;
            }

            base->header &= 1;
            m_data[m_reccnt++] = *base;
            bldr.AddKey(base->rec.key);

            if (m_bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                m_bf->insert(base->rec.key);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
    }

    TrieSpline(TrieSpline** shards, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0) {
        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(len);

        PriorityQueue<Wrapped<R>> pq(len);

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        
        for (size_t i = 0; i < len; ++i) {
            if (shards[i]) {
                auto base = shards[i]->get_data();
                cursors.emplace_back(Cursor{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
                attemp_reccnt += shards[i]->get_record_count();
                tombstone_count += shards[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);

                if (i == 0) {
                    m_max_key = shards[i]->m_max_key;
                    m_min_key = shards[i]->m_min_key;
                } else if (shards[i]->m_max_key > m_max_key) {
                    m_max_key = shards[i]->m_max_key;
                } else if (shards[i]->m_min_key < m_min_key) {
                    m_min_key = shards[i]->m_min_key;
                }
            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<K>(BF_FPR, tombstone_count, BF_HASH_FUNCS);
        auto bldr = ts::Builder<K>(m_min_key, m_max_key, g_max_error);

        size_t alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor<Wrapped<R>>(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor<Wrapped<R>>(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    bldr.AddKey(cursor.ptr->rec.key);
                    if (m_bf && cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        if (m_bf) m_bf->insert(cursor.ptr->rec.key);
                    }
                }
                pq.pop();
                
                if (advance_cursor<Wrapped<R>>(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            m_ts = bldr.Finalize();
        }
   }

    ~TrieSpline() {
        if (m_data) free(m_data);
        if (m_bf) delete m_bf;

    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if (filter && !m_bf->lookup(rec.key)) {
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
        return 0;
    }

private:

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

        return (m_data[idx].rec.key <= key) ? idx : m_reccnt;
    }

    Wrapped<R>* m_data;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    K m_max_key;
    K m_min_key;
    ts::TrieSpline<K> m_ts;
    BloomFilter<K> *m_bf;
};


template <RecordInterface R>
class TrieSplineRangeQuery {
public:
    static void *get_query_state(TrieSpline<R> *ts, void *parms) {
        auto res = new TrieSplineState<R>();
        auto p = (ts_range_query_parms<R> *) parms;

        res->start_idx = ts->get_lower_bound(p->lower_bound);
        res->stop_idx = ts->get_record_count();

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        auto res = new TrieSplineBufferState<R>();
        res->cutoff = buffer->get_record_count();

        return res;
    }

    static std::vector<Wrapped<R>> query(TrieSpline<R> *ts, void *q_state, void *parms) {
        std::vector<Wrapped<R>> records;
        auto p = (ts_range_query_parms<R> *) parms;
        auto s = (TrieSplineState<R> *) q_state;

        // if the returned index is one past the end of the
        // records for the TrieSpline, then there are not records
        // in the index falling into the specified range.
        if (s->start_idx == ts->get_record_count()) {
            return records;
        }

        auto ptr = ts->get_record_at(s->start_idx);
        size_t i = 0;
        while (ptr[i].rec.key <= p->upper_bound && i < s->stop_idx - s->start_idx) {
            records.emplace_back(ptr[i]);
            i++;
        }

        return records;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto p = (ts_range_query_parms<R> *) parms;
        auto s = (TrieSplineBufferState<R> *) state;

        std::vector<Wrapped<R>> records;
        for (size_t i=0; i<s->cutoff; i++) {
            auto rec = buffer->get_data() + i;
            if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                records.emplace_back(*rec);
            }

        }


        return records;
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results) {
        std::vector<R> output;

        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                output.emplace_back(results[i][j]);
            }
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (TrieSplineState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (TrieSplineBufferState<R> *) state;
        delete s;
    }
};

}