/*
 * include/shard/WIRS.h
 *
 * Copyright (C) 2023 Dong Xie <dongx@psu.edu>
 *                    Douglas Rumbaugh <drumbaugh@psu.edu>
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

thread_local size_t wirs_cancelations = 0;

template <WeightedRecordInterface R>
struct wirs_query_parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
    size_t sample_size;
    gsl_rng *rng;
};

template <WeightedRecordInterface R, bool Rejection>
class WIRSQuery;

template <WeightedRecordInterface R>
struct wirs_node {
    struct wirs_node<R> *left, *right;
    decltype(R::key) low, high;
    decltype(R::weight) weight;
    Alias* alias;
};

template <WeightedRecordInterface R>
struct WIRSState {
    decltype(R::weight) total_weight;
    std::vector<wirs_node<R>*> nodes;
    Alias* top_level_alias;
    size_t sample_size;

    WIRSState() {
        total_weight = 0;
        top_level_alias = nullptr;
    }

    ~WIRSState() {
        if (top_level_alias) delete top_level_alias;
    }
};

template <WeightedRecordInterface R>
struct WIRSBufferState {
    size_t cutoff;
    Alias* alias;
    std::vector<Wrapped<R>> records;
    decltype(R::weight) max_weight;
    size_t sample_size;
    decltype(R::weight) total_weight;

    ~WIRSBufferState() {
        delete alias;
    }

};

template <WeightedRecordInterface R>
class WIRS {
private:

    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:

    // FIXME: there has to be a better way to do this
    friend class WIRSQuery<R, true>;
    friend class WIRSQuery<R, false>;

    WIRS(MutableBuffer<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_root(nullptr) {

        size_t alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        m_bf = new BloomFilter<K>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        std::sort(base, stop, std::less<Wrapped<R>>());

        while (base < stop) {
            if (!(base->is_tombstone()) && (base + 1) < stop) {
                if (base->rec == (base + 1)->rec && (base + 1)->is_tombstone()) {
                    base += 2;
                    wirs_cancelations++;
                    continue;
                }
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            base->header &= 1;
            m_data[m_reccnt++] = *base;
            m_total_weight+= base->rec.weight;

            if (m_bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                m_bf->insert(base->rec.key);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
    }

    WIRS(WIRS** shards, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_root(nullptr) {
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
            build_wirs_structure();
        }
   }

    ~WIRS() {
        if (m_data) free(m_data);
        for (size_t i=0; i<m_alias.size(); i++) {
            if (m_alias[i]) delete m_alias[i];
        }

        if (m_bf) delete m_bf;

        free_tree(m_root);
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

    bool covered_by(struct wirs_node<R>* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[low_index].rec.key && m_data[high_index].rec.key < upper_key;
    }

    bool intersects(struct wirs_node<R>* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[high_index].rec.key && m_data[low_index].rec.key < upper_key;
    }

    void build_wirs_structure() {
        m_group_size = std::ceil(std::log(m_reccnt));
        size_t n_groups = std::ceil((double) m_reccnt / (double) m_group_size);
        
        // Fat point construction + low level alias....
        double sum_weight = 0.0;
        std::vector<W> weights;
        std::vector<double> group_norm_weight;
        size_t i = 0;
        size_t group_no = 0;
        while (i < m_reccnt) {
            double group_weight = 0.0;
            group_norm_weight.clear();
            for (size_t k = 0; k < m_group_size && i < m_reccnt; ++k, ++i) {
                auto w = m_data[i].rec.weight;
                group_norm_weight.emplace_back(w);
                group_weight += w;
                sum_weight += w;
            }

            for (auto& w: group_norm_weight)
                if (group_weight) w /= group_weight;
                else w = 1.0 / group_norm_weight.size();
            m_alias.emplace_back(new Alias(group_norm_weight));

            
            weights.emplace_back(group_weight);
        }

        assert(weights.size() == n_groups);

        m_root = construct_wirs_node(weights, 0, n_groups-1);
    }

     struct wirs_node<R>* construct_wirs_node(const std::vector<W>& weights, size_t low, size_t high) {
        if (low == high) {
            return new wirs_node<R>{nullptr, nullptr, low, high, weights[low], new Alias({1.0})};
        } else if (low > high) return nullptr;

        std::vector<double> node_weights;
        W sum = 0;
        for (size_t i = low; i < high; ++i) {
            node_weights.emplace_back(weights[i]);
            sum += weights[i];
        }

        for (auto& w: node_weights)
            if (sum) w /= sum;
            else w = 1.0 / node_weights.size();
        
        
        size_t mid = (low + high) / 2;
        return new wirs_node<R>{construct_wirs_node(weights, low, mid),
                                construct_wirs_node(weights, mid + 1, high),
                                low, high, sum, new Alias(node_weights)};
    }

    void free_tree(struct wirs_node<R>* node) {
        if (node) {
            delete node->alias;
            free_tree(node->left);
            free_tree(node->right);
            delete node;
        }
    }

    Wrapped<R>* m_data;
    std::vector<Alias *> m_alias;
    wirs_node<R>* m_root;
    W m_total_weight;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
    BloomFilter<K> *m_bf;
};


template <WeightedRecordInterface R, bool Rejection=true>
class WIRSQuery {
public:
    static void *get_query_state(WIRS<R> *wirs, void *parms) {
        auto res = new WIRSState<R>();
        decltype(R::key) lower_key = ((wirs_query_parms<R> *) parms)->lower_bound;
        decltype(R::key) upper_key = ((wirs_query_parms<R> *) parms)->upper_bound;

        // Simulate a stack to unfold recursion.        
        double total_weight = 0.0;
        struct wirs_node<R>* st[64] = {0};
        st[0] = wirs->m_root;
        size_t top = 1;
        while(top > 0) {
            auto now = st[--top];
            if (wirs->covered_by(now, lower_key, upper_key) ||
                (now->left == nullptr && now->right == nullptr && wirs->intersects(now, lower_key, upper_key))) {
                res->nodes.emplace_back(now);
                total_weight += now->weight;
            } else {
                if (now->left && wirs->intersects(now->left, lower_key, upper_key)) st[top++] = now->left;
                if (now->right && wirs->intersects(now->right, lower_key, upper_key)) st[top++] = now->right;
            }
        }
        
        std::vector<double> weights;
        for (const auto& node: res->nodes) {
            weights.emplace_back(node->weight / total_weight);
        }
        res->total_weight = total_weight;
        res->top_level_alias = new Alias(weights);
        res->sample_size = 0;

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        WIRSBufferState<R> *state = new WIRSBufferState<R>();
        auto parameters = (wirs_query_parms<R>*) parms;
        if constexpr (Rejection) {
            state->cutoff = buffer->get_record_count() - 1;
            state->max_weight = buffer->get_max_weight();
            state->total_weight = buffer->get_total_weight();
            state->sample_size = 0;
            return state;
        }

        std::vector<double> weights;

        state->cutoff = buffer->get_record_count() - 1;
        double total_weight = 0.0;

        for (size_t i = 0; i <= state->cutoff; i++) {
            auto rec = buffer->get_data() + i;

            if (rec->rec.key >= parameters->lower_bound && rec->rec.key <= parameters->upper_bound && !rec->is_tombstone() && !rec->is_deleted()) {
              weights.push_back(rec->rec.weight);
              state->records.push_back(*rec);
              total_weight += rec->rec.weight;
            }
        }

        for (size_t i = 0; i < weights.size(); i++) {
            weights[i] = weights[i] / total_weight;
        }

        state->total_weight = total_weight;
        state->alias = new Alias(weights);
        state->sample_size = 0;

        return state;
    }

    static void process_query_states(void *query_parms, std::vector<void*> shard_states, void *buff_state) {
        auto p = (wirs_query_parms<R> *) query_parms;
        auto bs = (WIRSBufferState<R> *) buff_state;

        std::vector<size_t> shard_sample_sizes(shard_states.size()+1, 0);
        size_t buffer_sz = 0;

        std::vector<decltype(R::weight)> weights;
        weights.push_back(bs->total_weight);

        decltype(R::weight) total_weight = 0;
        for (auto &s : shard_states) {
            auto state = (WIRSState<R> *) s;
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
            auto state = (WIRSState<R> *) shard_states[i];
            state->sample_size = shard_sample_sizes[i+1];
        }
    }



    static std::vector<Wrapped<R>> query(WIRS<R> *wirs, void *q_state, void *parms) { 
        auto lower_key = ((wirs_query_parms<R> *) parms)->lower_bound;
        auto upper_key = ((wirs_query_parms<R> *) parms)->upper_bound;
        auto rng = ((wirs_query_parms<R> *) parms)->rng;

        auto state = (WIRSState<R> *) q_state;
        auto sample_size = state->sample_size;

        std::vector<Wrapped<R>> result_set;

        if (sample_size == 0) {
            return result_set;
        }
        // k -> sampling: three levels. 1. select a node -> select a fat point -> select a record.
        size_t cnt = 0;
        size_t attempts = 0;
        do {
            ++attempts;
            // first level....
            auto node = state->nodes[state->top_level_alias->get(rng)];
            // second level...
            auto fat_point = node->low + node->alias->get(rng);
            // third level...
            size_t rec_offset = fat_point * wirs->m_group_size + wirs->m_alias[fat_point]->get(rng);
            auto record = wirs->m_data + rec_offset;

            // bounds rejection
            if (lower_key > record->rec.key || upper_key < record->rec.key) {
                continue;
            } 

            result_set.emplace_back(*record);
            cnt++;
        } while (attempts < sample_size);

        return result_set;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto st = (WIRSBufferState<R> *) state;
        auto p = (wirs_query_parms<R> *) parms;

        std::vector<Wrapped<R>> result;
        result.reserve(st->sample_size);

        if constexpr (Rejection) {
            for (size_t i=0; i<st->sample_size; i++) {
                auto idx = gsl_rng_uniform_int(p->rng, st->cutoff);
                auto rec = buffer->get_data() + idx;

                auto test = gsl_rng_uniform(p->rng) * st->max_weight;

                if (test <= rec->rec.weight && rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                    result.emplace_back(*rec);
                }
            }
            return result;
        }

        for (size_t i=0; i<st->sample_size; i++) {
            auto idx = st->alias->get(p->rng);
            result.emplace_back(st->records[idx]);
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
        auto s = (WIRSState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (WIRSBufferState<R> *) state;
        delete s;
    }


    //{q.get_buffer_query_state(p, p)};
    //{q.buffer_query(p, p)};

};

}
