/*
 * include/shard/AugBTree.h
 *
 * Copyright (C) 2023 Dong Xie <dongx@psu.edu>
 *                    Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the alias augmented B-tree. Designed to be 
 * used along side the WIRS query in include/query/wirs.h, but
 * also supports the necessary methods for other common query
 * types.
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once


#include <vector>
#include <cassert>

#include "framework/ShardRequirements.h"

#include "psu-ds/Alias.h"
#include "psu-ds/BloomFilter.h"
#include "util/bf_config.h"
#include "util/SortedMerge.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::Alias;
using psudb::byte;

namespace de {

template <WeightedRecordInterface R>
struct AugBTreeNode {
    struct AugBTreeNode<R> *left, *right;
    decltype(R::key) low, high;
    decltype(R::weight) weight;
    Alias* alias;
};

template <WeightedRecordInterface R>
class AugBTree {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:
    AugBTree(BufferView<R> buffer)
    : m_data(nullptr)
    , m_root(nullptr)
    , m_reccnt(0)
    , m_tombstone_cnt(0)
    , m_group_size(0)
    , m_alloc_size(0)
    , m_node_cnt(0)
    , m_bf(new BloomFilter<R>(BF_FPR, buffer.get_tombstone_count(), BF_HASH_FUNCS))
    {
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

        auto res = sorted_array_from_bufferview(std::move(buffer), m_data, m_bf);
        m_reccnt = res.record_count;
        m_tombstone_cnt = res.tombstone_count;

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
    }

    AugBTree(std::vector<AugBTree*> shards)
    : m_data(nullptr)
    , m_root(nullptr)
    , m_reccnt(0)
    , m_tombstone_cnt(0)
    , m_group_size(0)
    , m_alloc_size(0)
    , m_node_cnt(0)
    , m_bf(nullptr)
    {
        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        auto cursors = build_cursor_vec<R, AugBTree>(shards, &attemp_reccnt, &tombstone_count);

        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);

        auto res = sorted_array_merge<R>(cursors, m_data, m_bf);
        m_reccnt = res.record_count;
        m_tombstone_cnt = res.tombstone_count;

        if (m_reccnt > 0) {
            build_wirs_structure();
        }
   }

    ~AugBTree() {
        free(m_data);
        for (size_t i=0; i<m_alias.size(); i++) {
            delete m_alias[i];
        }

        delete m_bf;
        free_tree(m_root);
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if (filter && !m_bf->lookup(rec)) {
            return nullptr;
        }

        size_t idx = get_lower_bound(rec.key);
        if (idx >= m_reccnt) {
            return nullptr;
        }

        while (idx < (m_reccnt-1) && m_data[idx].rec < rec) ++idx;

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
        return m_alloc_size + m_node_cnt * sizeof(AugBTreeNode<Wrapped<R>>);
    }

    size_t get_aux_memory_usage() {
        return (m_bf) ? m_bf->memory_usage() : 0;
    }

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

    W find_covering_nodes(K lower_key, K upper_key, std::vector<void *> &nodes, std::vector<W> &weights) {
        W total_weight = 0;
        
        /* Simulate a stack to unfold recursion. */       
        struct AugBTreeNode<R>* st[64] = {0};
        st[0] = m_root;
        size_t top = 1;
        while(top > 0) {
            auto now = st[--top];
            if (covered_by(now, lower_key, upper_key) ||
                (now->left == nullptr && now->right == nullptr && intersects(now, lower_key, upper_key))) {
                nodes.emplace_back(now);
                weights.emplace_back(now->weight);
                total_weight += now->weight;
            } else {
                if (now->left && intersects(now->left, lower_key, upper_key)) st[top++] = now->left;
                if (now->right && intersects(now->right, lower_key, upper_key)) st[top++] = now->right;
            }
        }
        

        return total_weight;
    }

    Wrapped<R> *get_weighted_sample(K lower_key, K upper_key, void *internal_node, gsl_rng *rng) {
        /* k -> sampling: three levels. 1. select a node -> select a fat point -> select a record. */

        /* first level */ 
        auto node = (AugBTreeNode<R>*) internal_node;

        /* second level */
        auto fat_point = node->low + node->alias->get(rng);

        /* third level */
        size_t rec_offset = fat_point * m_group_size + m_alias[fat_point]->get(rng);
        auto record = m_data + rec_offset;

        /* bounds rejection */
        if (lower_key > record->rec.key || upper_key < record->rec.key) {
            return nullptr;
        } 

        return record;
    }

private:

    bool covered_by(struct AugBTreeNode<R>* node, const K& lower_key, const K& upper_key) {
        auto low_index = node->low * m_group_size;
        auto high_index = std::min((node->high + 1) * m_group_size - 1, m_reccnt - 1);
        return lower_key < m_data[low_index].rec.key && m_data[high_index].rec.key < upper_key;
    }

    bool intersects(struct AugBTreeNode<R>* node, const K& lower_key, const K& upper_key) {
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

        m_root = construct_AugBTreeNode(weights, 0, n_groups-1);
    }

     struct AugBTreeNode<R>* construct_AugBTreeNode(const std::vector<W>& weights, size_t low, size_t high) {
        if (low == high) {
            return new AugBTreeNode<R>{nullptr, nullptr, low, high, weights[low], new Alias({1.0})};
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
        
        m_node_cnt += 1; 
        size_t mid = (low + high) / 2;
        return new AugBTreeNode<R>{construct_AugBTreeNode(weights, low, mid),
                                construct_AugBTreeNode(weights, mid + 1, high),
                                low, high, sum, new Alias(node_weights)};
    }

    void free_tree(struct AugBTreeNode<R>* node) {
        if (node) {
            delete node->alias;
            free_tree(node->left);
            free_tree(node->right);
            delete node;
        }
    }

    Wrapped<R>* m_data;
    std::vector<Alias *> m_alias;
    AugBTreeNode<R>* m_root;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
    size_t m_alloc_size;
    size_t m_node_cnt;
    BloomFilter<R> *m_bf;
};
}
