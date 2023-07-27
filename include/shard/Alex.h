/*
 * include/shard/Alex.h
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

#include "alex.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/BloomFilter.h"
#include "util/bf_config.h"
#include "framework/MutableBuffer.h"
#include "framework/RecordInterface.h"
//#include "framework/ShardInterface.h"
//#include "framework/QueryInterface.h"

namespace de {

template <typename R>
struct alex_range_query_parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
};

template <typename R>
class AlexRangeQuery;

template <typename R>
struct AlexState {
    size_t start_idx;
    size_t stop_idx;
};

template <typename R>
struct AlexBufferState {
    size_t cutoff;
    Alias* alias;

    ~AlexBufferState() {
        delete alias;
    }
};


template <typename R, size_t epsilon=128>
class Alex {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

public:

    // FIXME: there has to be a better way to do this
    friend class AlexRangeQuery<R>;

    Alex(MutableBuffer<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0) {

        m_alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);
        std::vector<std::pair<K, V>> temp_records;

        m_bf = new BloomFilter<R>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        std::sort(base, stop, std::less<Wrapped<R>>());

        K min_key = base->rec.key;
        K max_key = (stop - 1)->rec.key;

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

            // FIXME: this shouldn't be necessary, but the tagged record
            // bypass doesn't seem to be working on this code-path, so this
            // ensures that tagged records from the buffer are able to be
            // dropped, eventually. It should only need to be &= 1
            base->header &= 3;
            m_data[m_reccnt++] = *base;
            temp_records.push_back({base->rec.key, base->rec.value});

            if (m_bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                m_bf->insert(base->rec);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            m_alex = alex::Alex<K, V>();
            m_alex.set_expected_insert_frac(0);
            m_alex.bulk_load(temp_records.data(), temp_records.size());
        }
    }

    Alex(Alex** shards, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0) {
        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(len);

        PriorityQueue<Wrapped<R>> pq(len);

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        
        for (size_t i = 0; i < len; ++i) {
            if (shards[i]) {
                auto base = shards[i]->get_data();
                cursors.emplace_back(Cursor<Wrapped<R>>{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
                attemp_reccnt += shards[i]->get_record_count();
                tombstone_count += shards[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);

            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);

        m_alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);

        std::vector<std::pair<K, V>> temp_records;

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
                    temp_records.push_back({cursor.ptr->rec.key, cursor.ptr->rec.value});
                    if (m_bf && cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        if (m_bf) m_bf->insert(cursor.ptr->rec);
                    }
                }
                pq.pop();
                
                if (advance_cursor<Wrapped<R>>(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            m_alex = alex::Alex<K, V>();
            m_alex.set_expected_insert_frac(0);
            m_alex.bulk_load(temp_records.data(), temp_records.size());
        }
   }

    ~Alex() {
        if (m_data) free(m_data);
        if (m_bf) delete m_bf;

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
        return m_alex.model_size() + m_alex.data_size();
    }

    typename alex::Alex<K, V>::Iterator get_lower_bound(const K& key) const {
        auto bound = m_alex.find(key);
        while (bound != m_alex.end() && bound.key() < key) {
            bound++;
        }

        return bound;
    }

private:
    Wrapped<R>* m_data;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_alloc_size;
    K m_max_key;
    K m_min_key;
    alex::Alex<K, V> m_alex;
    BloomFilter<R> *m_bf;
};


template <typename R>
class AlexRangeQuery {
public:
    static void *get_query_state(Alex<R> *ts, void *parms) {
        auto res = new AlexState<R>();
        auto p = (alex_range_query_parms<R> *) parms;

        res->start_idx = ts->get_lower_bound(p->lower_bound);
        res->stop_idx = ts->get_record_count();

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        auto res = new AlexBufferState<R>();
        res->cutoff = buffer->get_record_count();

        return res;
    }

    static void process_query_states(void *query_parms, std::vector<void*> shard_states, void *buff_state) {
        return;
    }

    static std::vector<Wrapped<R>> query(Alex<R> *ts, void *q_state, void *parms) {
        std::vector<Wrapped<R>> records;
        auto p = (alex_range_query_parms<R> *) parms;
        auto s = (AlexState<R> *) q_state;

        // if the returned index is one past the end of the
        // records for the Alex, then there are not records
        // in the index falling into the specified range.
        if (s->start_idx == ts->get_record_count()) {
            return records;
        }

        auto ptr = ts->get_record_at(s->start_idx);
        
        // roll the pointer forward to the first record that is
        // greater than or equal to the lower bound.
        while(ptr->rec.key < p->lower_bound) {
            ptr++;
        }

        while (ptr->rec.key <= p->upper_bound && ptr < ts->m_data + s->stop_idx) {
            records.emplace_back(*ptr);
            ptr++;
        }

        return records;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto p = (alex_range_query_parms<R> *) parms;
        auto s = (AlexBufferState<R> *) state;

        std::vector<Wrapped<R>> records;
        for (size_t i=0; i<s->cutoff; i++) {
            auto rec = buffer->get_data() + i;
            if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                records.emplace_back(*rec);
            }
        }

        return records;
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results, void *parms) {
        size_t total = 0;
        for (size_t i=0; i<results.size(); i++) {
            total += results[i].size();
        }

        if (total == 0) {
            return std::vector<R>();
        }

        std::vector<R> output;
        output.reserve(total);

        for (size_t i=0; i<results.size(); i++) {
            std::move(results[i].begin(), results[i].end(), std::back_inserter(output));
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (AlexState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (AlexBufferState<R> *) state;
        delete s;
    }
};

;

}
