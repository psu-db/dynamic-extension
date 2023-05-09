/*
 * include/shard/WIRS.h
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

#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/Alias.h"
#include "ds/BloomFilter.h"
#include "util/Record.h"
#include "framework/MutableBuffer.h"

namespace de {

thread_local size_t wirs_cancelations = 0;

template <typename K, typename V, typename W>
class WIRS {
private:
    struct wirs_node {
        struct wirs_node *left, *right;
        K low, high;
        W weight;
        Alias* alias;
    };

    struct WIRSState {
        W tot_weight;
        std::vector<wirs_node*> nodes;
        Alias* top_level_alias;

        ~WIRSState() {
            if (top_level_alias) delete top_level_alias;
        }
    };

public:
    WIRS(MutableBuffer<K, V, W>* buffer, BloomFilter* bf, bool tagging)
    : m_reccnt(0), m_tombstone_cnt(0), m_deleted_cnt(0), m_total_weight(0), m_rejection_cnt(0), 
      m_ts_check_cnt(0), m_tagging(tagging), m_root(nullptr) {

        size_t alloc_size = (buffer->get_record_count() * sizeof(Record<K, V, W>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Record<K, V, W>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Record<K, V, W>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->sorted_output();
        auto stop = base + buffer->get_record_count();

        while (base < stop) {
            if (!m_tagging) {
                if (!(base->is_tombstone()) && (base + 1) < stop) {
                    if (base->match(base + 1) && (base + 1)->is_tombstone()) {
                        base += 2;
                        wirs_cancelations++;
                        continue;
                    }
                }
            } else if (base->get_delete_status()) {
                base += 1;
                continue;
            }

            base->header &= 1;
            m_data[m_reccnt++] = *base;
            m_total_weight+= base->weight;

            if (bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                bf->insert(base->key);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
    }

    WIRS(WIRS** shards, size_t len, BloomFilter* bf, bool tagging)
    : m_reccnt(0), m_tombstone_cnt(0), m_deleted_cnt(0), m_total_weight(0), m_rejection_cnt(0), m_ts_check_cnt(0), 
      m_tagging(tagging), m_root(nullptr) {
        std::vector<Cursor<K,V,W>> cursors;
        cursors.reserve(len);

        PriorityQueue<K, V, W> pq(len);

        size_t attemp_reccnt = 0;
        
        for (size_t i = 0; i < len; ++i) {
            if (shards[i]) {
                auto base = shards[i]->sorted_output();
                cursors.emplace_back(Cursor{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
                attemp_reccnt += shards[i]->get_record_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor<K,V,W>{nullptr, nullptr, 0, 0});
            }
        }

        size_t alloc_size = (attemp_reccnt * sizeof(Record<K, V, W>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Record<K, V, W>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Record<K, V, W>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<K, V, W>{nullptr, 0};
            if (!m_tagging && !now.data->is_tombstone() && next.data != nullptr &&
                now.data->match(next.data) && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor<K,V,W>(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor<K,V,W>(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!m_tagging || !cursor.ptr->get_delete_status()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    m_total_weight += cursor.ptr->weight;
                    if (bf && cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        if (bf) bf->insert(cursor.ptr->key);
                    }
                }
                pq.pop();
                
                if (advance_cursor<K,V,W>(cursor)) pq.push(cursor.ptr, now.version);
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

        free_tree(m_root);
    }

    bool delete_record(const K& key, const V& val) {
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        while (idx < m_reccnt && m_data[idx].lt(key, val)) ++idx;

        if (m_data[idx].match(key, val, false)) {
            m_data[idx].set_delete_status();
            m_deleted_cnt++;
            return true;
        }

        return false;
    }

    void free_tree(struct wirs_node* node) {
        if (node) {
            delete node->alias;
            free_tree(node->left);
            free_tree(node->right);
            delete node;
        }
    }

    Record<K, V, W>* sorted_output() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    const Record<K, V, W>* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx;
    }

    // low - high -> decompose to a set of nodes.
    // Build Alias across the decomposed nodes.
    WIRSState* get_sample_shard_state(const K& lower_key, const K& upper_key) {
        WIRSState* res = new WIRSState();

        // Simulate a stack to unfold recursion.        
        double tot_weight = 0.0;
        struct wirs_node* st[64] = {0};
        st[0] = m_root;
        size_t top = 1;
        while(top > 0) {
            auto now = st[--top];
            if (covered_by(now, lower_key, upper_key) ||
                (now->left == nullptr && now->right == nullptr && intersects(now, lower_key, upper_key))) {
                res->nodes.emplace_back(now);
                tot_weight += now->weight;
            } else {
                if (now->left && intersects(now->left, lower_key, upper_key)) st[top++] = now->left;
                if (now->right && intersects(now->right, lower_key, upper_key)) st[top++] = now->right;
            }
        }
        
        std::vector<double> weights;
        for (const auto& node: res->nodes) {
            weights.emplace_back(node->weight / tot_weight);
        }
        res->tot_weight = tot_weight;
        res->top_level_alias = new Alias(weights);

        return res;
    }

    static void delete_state(void *state) {
        auto s = (WIRSState *) state;
        delete s;
    }

    // returns the number of records sampled
    // NOTE: This operation returns records strictly between the lower and upper bounds, not
    // including them.
    size_t get_samples(void* shard_state, std::vector<Record<K, V, W>> &result_set, const K& lower_key, const K& upper_key, size_t sample_sz, gsl_rng *rng) {
        WIRSState *state = (WIRSState *) shard_state;
        if (sample_sz == 0) {
            return 0;
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
            size_t rec_offset = fat_point * m_group_size + m_alias[fat_point]->get(rng);
            auto record = m_data + rec_offset;

            // bounds rejection
            if (lower_key > record->key || upper_key < record->key) {
                continue;
            } 

            result_set.emplace_back(*record);
            cnt++;
        } while (attempts < sample_sz);

        return cnt;
    }

    size_t get_lower_bound(const K& key) const {
        size_t min = 0;
        size_t max = m_reccnt - 1;

        const char * record_key;
        while (min < max) {
            size_t mid = (min + max) / 2;

            if (key > m_data[mid].key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return min;
    }

    bool check_delete(K key, V val) {
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        auto ptr = m_data + get_lower_bound(key);

        while (ptr < m_data + m_reccnt && ptr->lt(key, val)) {
            ptr ++;
        }

        bool result = (m_tagging) ? ptr->get_delete_status()
                                  : ptr->match(key, val, true);
        m_rejection_cnt += result;
        return result;
    }

    bool check_tombstone(const K& key, const V& val) {
        m_ts_check_cnt++;
        size_t idx = get_lower_bound(key);
        if (idx >= m_reccnt) {
            return false;
        }

        auto ptr = m_data + get_lower_bound(key);

        while (ptr < m_data + m_reccnt && ptr->lt(key, val)) {
            ptr ++;
        }

        bool result = ptr->match(key, val, true);
        m_rejection_cnt += result;

        return result;
    }


    size_t get_memory_utilization() {
        return 0;
    }


    size_t get_rejection_count() {
        return m_rejection_cnt;
    }

    size_t get_ts_check_count() {
        return m_ts_check_cnt;
    }
    
private:

    bool covered_by(struct wirs_node* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[low_index].key && m_data[high_index].key < upper_key;
    }

    bool intersects(struct wirs_node* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[high_index].key && m_data[low_index].key < upper_key;
    }
    
    struct wirs_node* construct_wirs_node(const std::vector<W>& weights, size_t low, size_t high) {
        if (low == high) {
            return new wirs_node{nullptr, nullptr, low, high, weights[low], new Alias({1.0})};
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
        return new wirs_node{construct_wirs_node(weights, low, mid),
                             construct_wirs_node(weights, mid + 1, high),
                             low, high, sum, new Alias(node_weights)};
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
                auto w = m_data[i].weight;
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

    Record<K, V, W>* m_data;
    std::vector<Alias *> m_alias;
    wirs_node* m_root;
    bool m_tagging;
    W m_total_weight;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
    size_t m_ts_check_cnt;
    size_t m_deleted_cnt;

    // The number of rejections caused by tombstones
    // in this WIRS.
    size_t m_rejection_cnt;
};

}
