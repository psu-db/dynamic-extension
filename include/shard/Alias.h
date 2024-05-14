/*
 * include/shard/Alias.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the psudb::Alias Walker's Alias
 * structure. Designed to be used along side the WSS
 * query in include/query/wss.h
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once

#include <vector>

#include "framework/ShardRequirements.h"

#include "psu-ds/Alias.h"
#include "psu-ds/BloomFilter.h"
#include "util/bf_config.h"
#include "util/SortedMerge.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::byte;

namespace de {

static thread_local size_t wss_cancelations = 0;

template <WeightedRecordInterface R>
class Alias {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:
    Alias(BufferView<R> buffer)
        : m_data(nullptr)
        , m_alias(nullptr)
        , m_total_weight(0)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_bf(new BloomFilter<R>(BF_FPR, buffer.get_tombstone_count(), BF_HASH_FUNCS)) {
                       

        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

        auto res = sorted_array_from_bufferview<R>(std::move(buffer), m_data, m_bf);
        m_reccnt = res.record_count;
        m_tombstone_cnt = res.tombstone_count;

        if (m_reccnt > 0) {
            std::vector<W> weights;
            for (size_t i=0; i<m_reccnt; i++) {
                weights.emplace_back(m_data[i].rec.weight);
                m_total_weight += m_data[i].rec.weight;
            }

            build_alias_structure(weights);
        }
    }

    Alias(std::vector<Alias*> &shards)
        : m_data(nullptr)
        , m_alias(nullptr)
        , m_total_weight(0)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_bf(nullptr) {

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        auto cursors = build_cursor_vec<R, Alias>(shards, &attemp_reccnt, &tombstone_count);

        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);

        auto res = sorted_array_merge<R>(cursors, m_data, m_bf);
        m_reccnt = res.record_count;
        m_tombstone_cnt = res.tombstone_count;

        if (m_reccnt > 0) {
            std::vector<W> weights;
            for (size_t i=0; i<m_reccnt; i++) {
                weights.emplace_back(m_data[i].rec.weight);
                m_total_weight += m_data[i].rec.weight;
            }

            build_alias_structure(weights);
        }
   }

    ~Alias() {
        free(m_data);
        delete m_alias;
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
        return 0;
    }

    size_t get_aux_memory_usage() {
        return (m_bf) ? m_bf->memory_usage() : 0;
    }

    W get_total_weight() {
        return m_total_weight;
    }

    size_t get_weighted_sample(gsl_rng *rng) const {
        return m_alias->get(rng);
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

private:

    void build_alias_structure(std::vector<W> &weights) {

        // normalize the weights vector
        std::vector<double> norm_weights(weights.size());

        for (size_t i=0; i<weights.size(); i++) {
            norm_weights[i] = (double) weights[i] / (double) m_total_weight;
        }

        // build the alias structure
        m_alias = new psudb::Alias(norm_weights);
    }

    Wrapped<R>* m_data;
    psudb::Alias *m_alias;
    W m_total_weight;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_alloc_size;
    BloomFilter<R> *m_bf;
};
}
