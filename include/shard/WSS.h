/*
    {s.get_tombstone_count()} -> std::convertible_to<size_t>;
 * include/shard/WSS.h
 *
 * Copyright (C) 2023 Dong Xie <dongx@psu.edu>
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

#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/Alias.h"
#include "ds/BloomFilter.h"
#include "util/bf_config.h"
#include "framework/MutableBuffer.h"
#include "framework/RecordInterface.h"
#include "framework/ShardInterface.h"
#include "framework/QueryInterface.h"

namespace de {

thread_local size_t wss_cancelations = 0;

template <WeightedRecordInterface R>
struct wss_query_parms {
    size_t sample_size;
    gsl_rng *rng;
};

template <WeightedRecordInterface R, bool Rejection>
class WSSQuery;

template <WeightedRecordInterface R>
struct WSSState {
    decltype(R::weight) total_weight;
    size_t sample_size;

    WSSState() {
        total_weight = 0;
    }
};

template <WeightedRecordInterface R>
struct WSSBufferState {
    size_t cutoff;
    size_t sample_size;
    Alias* alias;
    decltype(R::weight) max_weight;
    decltype(R::weight) total_weight;

    ~WSSBufferState() {
        delete alias;
    }

};

template <WeightedRecordInterface R>
class WSS {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:

    // FIXME: there has to be a better way to do this
    friend class WSSQuery<R, true>;
    friend class WSSQuery<R, false>;

    WSS(MutableBuffer<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_alias(nullptr), m_bf(nullptr) {

        size_t alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        m_bf = new BloomFilter<K>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        std::sort(base, stop, std::less<Wrapped<R>>());

        std::vector<W> weights;

        while (base < stop) {
            if (!(base->is_tombstone()) && (base + 1) < stop) {
                if (base->rec == (base + 1)->rec && (base + 1)->is_tombstone()) {
                    base += 2;
                    wss_cancelations++;
                    continue;
                }
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            base->header &= 1;
            m_data[m_reccnt++] = *base;
            m_total_weight+= base->rec.weight;
            weights.push_back(base->rec.weight);

            if (m_bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                m_bf->insert(base->rec.key);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            build_alias_structure(weights);
        }
    }

    WSS(WSS** shards, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_alias(nullptr), m_bf(nullptr) {
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
            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<K>(BF_FPR, tombstone_count, BF_HASH_FUNCS);

        size_t alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        std::vector<W> weights;
        
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
                    m_total_weight += cursor.ptr->rec.weight;
                    weights.push_back(cursor.ptr->rec.weight);
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
            build_alias_structure(weights);
        }
   }

    ~WSS() {
        if (m_data) free(m_data);
        if (m_alias) delete m_alias;
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
        size_t min = 0;
        size_t max = m_reccnt - 1;

        const char * record_key;
        while (min < max) {
            size_t mid = (min + max) / 2;

            if (key > m_data[mid].rec.key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return min;
    }

    void build_alias_structure(std::vector<W> &weights) {

        // normalize the weights vector
        std::vector<double> norm_weights(weights.size());

        for (size_t i=0; i<weights.size(); i++) {
            norm_weights[i] = (double) weights[i] / (double) m_total_weight;
        }

        // build the alias structure
        m_alias = new Alias(norm_weights);
    }

    Wrapped<R>* m_data;
    Alias *m_alias;
    W m_total_weight;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
    BloomFilter<K> *m_bf;
};


template <WeightedRecordInterface R, bool Rejection=true>
class WSSQuery {
public:
    static void *get_query_state(WSS<R> *wss, void *parms) {
        auto res = new WSSState<R>();
        res->total_weight = wss->m_total_weight;
        res->sample_size = 0;

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        WSSBufferState<R> *state = new WSSBufferState<R>();
        auto parameters = (wss_query_parms<R>*) parms;
        if constexpr (Rejection) {
            state->cutoff = buffer->get_record_count() - 1;
            state->max_weight = buffer->get_max_weight();
            state->total_weight = buffer->get_total_weight();
            return state;
        }

        std::vector<double> weights;

        state->cutoff = buffer->get_record_count() - 1;
        double total_weight = 0.0;

        for (size_t i = 0; i <= state->cutoff; i++) {
            auto rec = buffer->get_data() + i;
            weights.push_back(rec->rec.weight);
            total_weight += rec->rec.weight;
        }

        for (size_t i = 0; i < weights.size(); i++) {
            weights[i] = weights[i] / total_weight;
        }

        state->alias = new Alias(weights);
        state->total_weight = total_weight;

        return state;
    }

    static void process_query_states(void *query_parms, std::vector<void*> shard_states, void *buff_state) {
        auto p = (wss_query_parms<R> *) query_parms;
        auto bs = (WSSBufferState<R> *) buff_state;

        std::vector<size_t> shard_sample_sizes(shard_states.size()+1, 0);
        size_t buffer_sz = 0;

        std::vector<decltype(R::weight)> weights;
        weights.push_back(bs->total_weight);

        decltype(R::weight) total_weight = 0;
        for (auto &s : shard_states) {
            auto state = (WSSState<R> *) s;
            total_weight += state->total_weight;
            weights.push_back(state->total_weight);
        }

        std::vector<double> normalized_weights;
        for (auto w : weights) {
            normalized_weights.push_back((double) w / (double) total_weight);
        }

        auto shard_alias = Alias(normalized_weights);
        for (size_t i=0; i<p->sample_size; i++) {
            auto idx = shard_alias.get(p->rng);            
            if (idx == 0) {
                buffer_sz++;
            } else {
                shard_sample_sizes[idx - 1]++;
            }
        }


        bs->sample_size = buffer_sz;
        for (size_t i=0; i<shard_states.size(); i++) {
            auto state = (WSSState<R> *) shard_states[i];
            state->sample_size = shard_sample_sizes[i+1];
        }
    }

    static std::vector<Wrapped<R>> query(WSS<R> *wss, void *q_state, void *parms) { 
        auto rng = ((wss_query_parms<R> *) parms)->rng;

        auto state = (WSSState<R> *) q_state;
        auto sample_size = state->sample_size;

        std::vector<Wrapped<R>> result_set;

        if (sample_size == 0) {
            return result_set;
        }
        size_t attempts = 0;
        do {
            attempts++;
            size_t idx = wss->m_alias->get(rng);
            result_set.emplace_back(*wss->get_record_at(idx));
        } while (attempts < sample_size);

        return result_set;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto st = (WSSBufferState<R> *) state;
        auto p = (wss_query_parms<R> *) parms;

        std::vector<Wrapped<R>> result;
        result.reserve(st->sample_size);

        if constexpr (Rejection) {
            for (size_t i=0; i<st->sample_size; i++) {
                auto idx = gsl_rng_uniform_int(p->rng, st->cutoff);
                auto rec = buffer->get_data() + idx;

                auto test = gsl_rng_uniform(p->rng) * st->max_weight;

                if (test <= rec->rec.weight) {
                    result.emplace_back(*rec);
                }
            }
            return result;
        }

        for (size_t i=0; i<st->sample_size; i++) {
            auto idx = st->alias->get(p->rng);
            result.emplace_back(*(buffer->get_data() + idx));
        }

        return result;
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
        auto s = (WSSState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (WSSBufferState<R> *) state;
        delete s;
    }
};

}
