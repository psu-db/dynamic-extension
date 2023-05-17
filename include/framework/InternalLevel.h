/*
 * include/framework/InternalLevel.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <memory>

#include "util/types.h"
#include "util/bf_config.h"
#include "framework/ShardInterface.h"
#include "framework/MutableBuffer.h"
#include "ds/BloomFilter.h"

namespace de {

template <RecordInterface R, ShardInterface S>
class InternalLevel {

    static const size_t REJECTION_TRIGGER_THRESHOLD = 1024;

    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    //typedef WIRS<R> S;

private:
    struct InternalLevelStructure {
        InternalLevelStructure(size_t cap)
        : m_cap(cap)
        , m_shards(new S*[cap]{nullptr})
        , m_bfs(new BloomFilter*[cap]{nullptr}) {} 

        ~InternalLevelStructure() {
            for (size_t i = 0; i < m_cap; ++i) {
                if (m_shards[i]) delete m_shards[i];
                if (m_bfs[i]) delete m_bfs[i];
            }

            delete[] m_shards;
            delete[] m_bfs;
        }

        size_t m_cap;
        S** m_shards;
        BloomFilter** m_bfs;
    };

public:
    InternalLevel(ssize_t level_no, size_t shard_cap)
    : m_level_no(level_no), m_shard_cnt(0)
    , m_structure(new InternalLevelStructure(shard_cap)) {}

    // Create a new memory level sharing the shards and repurposing it as previous level_no + 1
    // WARNING: for leveling only.
    InternalLevel(InternalLevel* level)
    : m_level_no(level->m_level_no + 1), m_shard_cnt(level->m_shard_cnt)
    , m_structure(level->m_structure) {
        assert(m_structure->m_cap == 1 && m_shard_cnt == 1);
    }

    ~InternalLevel() {}

    // WARNING: for leveling only.
    // assuming the base level is the level new level is merging into. (base_level is larger.)
    static InternalLevel* merge_levels(InternalLevel* base_level, InternalLevel* new_level, const gsl_rng* rng) {
        assert(base_level->m_level_no > new_level->m_level_no || (base_level->m_level_no == 0 && new_level->m_level_no == 0));
        auto res = new InternalLevel(base_level->m_level_no, 1);
        res->m_shard_cnt = 1;
        res->m_structure->m_bfs[0] =
            new BloomFilter(BF_FPR,
                            new_level->get_tombstone_count() + base_level->get_tombstone_count(),
                            BF_HASH_FUNCS, rng);
        S* shards[2];
        shards[0] = base_level->m_structure->m_shards[0];
        shards[1] = new_level->m_structure->m_shards[0];

        res->m_structure->m_shards[0] = new S(shards, 2, res->m_structure->m_bfs[0]);
        return res;
    }

    void append_mem_table(MutableBuffer<R>* buffer, const gsl_rng* rng) {
        assert(m_shard_cnt < m_structure->m_cap);
        m_structure->m_bfs[m_shard_cnt] = new BloomFilter(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS, rng);
        m_structure->m_shards[m_shard_cnt] = new S(buffer, m_structure->m_bfs[m_shard_cnt]);
        ++m_shard_cnt;
    }

    void append_merged_shards(InternalLevel* level, const gsl_rng* rng) {
        assert(m_shard_cnt < m_structure->m_cap);
        m_structure->m_bfs[m_shard_cnt] = new BloomFilter(BF_FPR, level->get_tombstone_count(), BF_HASH_FUNCS, rng);
        m_structure->m_shards[m_shard_cnt] = new S(level->m_structure->m_shards, level->m_shard_cnt, m_structure->m_bfs[m_shard_cnt]);
        ++m_shard_cnt;
    }

    S *get_merged_shard() {
        S *shards[m_shard_cnt];

        for (size_t i=0; i<m_shard_cnt; i++) {
            shards[i] = (m_structure->m_shards[i]) ? m_structure->m_shards[i] : nullptr;
        }

        return new S(shards, m_shard_cnt, nullptr);
    }

    // Append the sample range in-order.....
    void get_query_states(std::vector<uint64_t>& weights, std::vector<std::pair<ShardID, S *>> &shards, std::vector<void*>& shard_states, void *query_parms) {
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_structure->m_shards[i]) {
                auto shard_state = m_structure->m_shards[i]->get_query_state(query_parms);
                if (shard_state->tot_weight > 0) {
                    shards.push_back({{m_level_no, (ssize_t) i}, m_structure->m_shards[i]});
                    weights.push_back(shard_state->tot_weight);
                    shard_states.emplace_back(shard_state);
                } else {
                    S::delete_state(shard_state);
                }
            }
        }
    }

    bool bf_rejection_check(size_t shard_stop, const K& key) {
        for (size_t i = 0; i < shard_stop; ++i) {
            if (m_structure->m_bfs[i] && m_structure->m_bfs[i]->lookup(key))
                return true;
        }
        return false;
    }

    bool check_tombstone(size_t shard_stop, const R& rec) {
        if (m_shard_cnt == 0) return false;

        for (int i = m_shard_cnt - 1; i >= (ssize_t) shard_stop;  i--) {
            if (m_structure->m_shards[i] && (m_structure->m_bfs[i]->lookup(rec.key))
                && m_structure->m_shards[i]->check_tombstone(rec))
                return true;
        }
        return false;
    }

    bool delete_record(const K& key, const V& val) {
        for (size_t i = 0; i < m_structure->m_cap;  ++i) {
            if (m_structure->m_shards[i] && m_structure->m_shards[i]->delete_record(key, val)) {
                return true;
            }
        }

        return false;
    }

    const R* get_record_at(size_t shard_no, size_t idx) {
        return m_structure->m_shards[shard_no]->get_record_at(idx);
    }
    
    S* get_shard(size_t idx) {
        return m_structure->m_shards[idx];
    }

    size_t get_shard_count() {
        return m_shard_cnt;
    }

    size_t get_record_cnt() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            cnt += m_structure->m_shards[i]->get_record_count();
        }

        return cnt;
    }
    
    size_t get_tombstone_count() {
        size_t res = 0;
        for (size_t i = 0; i < m_shard_cnt; ++i) {
            res += m_structure->m_shards[i]->get_tombstone_count();
        }
        return res;
    }

    size_t get_aux_memory_utilization() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_structure->m_bfs[i]) {
                cnt += m_structure->m_bfs[i]->get_memory_utilization();
            }
        }

        return cnt;
    }

    size_t get_memory_utilization() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_structure->m_shards[i]) {
                cnt += m_structure->m_shards[i]->get_memory_utilization();
            }
        }

        return cnt;
    }

    double get_tombstone_prop() {
        size_t tscnt = 0;
        size_t reccnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_structure->m_shards[i]) {
                tscnt += m_structure->m_shards[i]->get_tombstone_count();
                reccnt += m_structure->m_shards[i]->get_record_count();
            }
        }

        return (double) tscnt / (double) (tscnt + reccnt);
    }

    size_t get_rejection_count() {
        size_t rej_cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_structure->m_shards[i]) {
                rej_cnt += m_structure->m_shards[i]->get_rejection_count();
            }
        }

        return rej_cnt;
    }

    double get_rejection_rate() {
        size_t rej_cnt = 0;
        size_t attempt_cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_structure->m_shards[i]) {
                attempt_cnt += m_structure->m_shards[i]->get_ts_check_count();
                rej_cnt += m_structure->m_shards[i]->get_rejection_count();
            }
        }

        if (attempt_cnt == 0) return 0;

        // the rejection rate is considered 0 until we exceed an
        // absolute threshold of rejections.
        if (rej_cnt <= REJECTION_TRIGGER_THRESHOLD) return 0;

        return (double) rej_cnt / (double) attempt_cnt;
    }

private:
    ssize_t m_level_no;
    
    size_t m_shard_cnt;
    size_t m_shard_size_cap;
    std::shared_ptr<InternalLevelStructure> m_structure;
};

}
