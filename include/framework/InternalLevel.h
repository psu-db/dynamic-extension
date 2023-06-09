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
#include "framework/ShardInterface.h"
#include "framework/QueryInterface.h"
#include "framework/RecordInterface.h"
#include "framework/MutableBuffer.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q>
class InternalLevel {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;
public:
    InternalLevel(ssize_t level_no, size_t shard_cap)
    : m_level_no(level_no)
    , m_shard_cnt(0)
    , m_shards(shard_cap, nullptr)
    , m_owns(shard_cap, true)
    {}

    // Create a new memory level sharing the shards and repurposing it as previous level_no + 1
    // WARNING: for leveling only.
    InternalLevel(InternalLevel* level)
    : m_level_no(level->m_level_no + 1)
    , m_shard_cnt(level->m_shard_cnt)
    , m_shards(level->m_shards.size(), nullptr)
    , m_owns(level->m_owns.size(), true) {
        assert(m_shard_cnt == 1 && m_shards.size() == 1);

        for (size_t i=0; i<m_shards.size(); i++) {
            level->m_owns[i] = false;
            m_shards[i] = level->m_shards[i];
        }
    }

    ~InternalLevel() { 
        for (size_t i=0; i<m_shards.size(); i++) {
            if (m_owns[i]) delete m_shards[i];
        }
    }

    // WARNING: for leveling only.
    // assuming the base level is the level new level is merging into. (base_level is larger.)
    static InternalLevel* merge_levels(InternalLevel* base_level, InternalLevel* new_level) {
        assert(base_level->m_level_no > new_level->m_level_no || (base_level->m_level_no == 0 && new_level->m_level_no == 0));
        auto res = new InternalLevel(base_level->m_level_no, 1);
        res->m_shard_cnt = 1;
        Shard* shards[2];
        shards[0] = base_level->m_shards[0];
        shards[1] = new_level->m_shards[0];

        res->m_shards[0] = new S(shards, 2);
        return res;
    }

    void append_buffer(Buffer* buffer) {
        assert(m_shard_cnt < m_shards.size());
        m_shards[m_shard_cnt] = new S(buffer);
        m_owns[m_shard_cnt] = true;
        ++m_shard_cnt;
    }

    void append_merged_shards(InternalLevel* level) {
        assert(m_shard_cnt < m_shards.size());
        m_shards[m_shard_cnt] = new S(level->m_shards.data(), level->m_shard_cnt);
        m_owns[m_shard_cnt] = true;

        ++m_shard_cnt;
    }

    Shard *get_merged_shard() {
        Shard *shards[m_shard_cnt];

        for (size_t i=0; i<m_shard_cnt; i++) {
            shards[i] = m_shards[i];
        }

        return new S(shards, m_shard_cnt);
    }

    // Append the sample range in-order.....
    void get_query_states(std::vector<std::pair<ShardID, Shard *>> &shards, std::vector<void*>& shard_states, void *query_parms) {
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_shards[i]) {
                auto shard_state = Q::get_query_state(m_shards[i], query_parms);
                shards.push_back({{m_level_no, (ssize_t) i}, m_shards[i]});
                shard_states.emplace_back(shard_state);
            }
        }
    }

    bool check_tombstone(size_t shard_stop, const R& rec) {
        if (m_shard_cnt == 0) return false;

        for (int i = m_shard_cnt - 1; i >= (ssize_t) shard_stop;  i--) {
            if (m_shards[i]) {
                auto res = m_shards[i]->point_lookup(rec, true);
                if (res && res->is_tombstone()) {
                    return true;
                }
            }
        }
        return false;
    }

    bool delete_record(const R &rec) {
        if (m_shard_cnt == 0) return false;

        for (size_t i = 0; i < m_shards.size();  ++i) {
            if (m_shards[i]) {
                auto res = m_shards[i]->point_lookup(rec);
                if (res) {
                    res->set_delete();
                }
            }
        }

        return false;
    }

    Shard* get_shard(size_t idx) {
        return m_shards[idx];
    }

    size_t get_shard_count() {
        return m_shard_cnt;
    }

    size_t get_record_count() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            cnt += m_shards[i]->get_record_count();
        }

        return cnt;
    }
    
    size_t get_tombstone_count() {
        size_t res = 0;
        for (size_t i = 0; i < m_shard_cnt; ++i) {
            res += m_shards[i]->get_tombstone_count();
        }
        return res;
    }

    size_t get_aux_memory_usage() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            cnt += m_shards[i]->get_aux_memory_usage();
        }

        return cnt;
    }

    size_t get_memory_usage() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_shards[i]) {
                cnt += m_shards[i]->get_memory_usage();
            }
        }

        return cnt;
    }

    double get_tombstone_prop() {
        size_t tscnt = 0;
        size_t reccnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_shards[i]) {
                tscnt += m_shards[i]->get_tombstone_count();
                reccnt += (*m_shards[i])->get_record_count();
            }
        }

        return (double) tscnt / (double) (tscnt + reccnt);
    }

private:
    ssize_t m_level_no;
    
    size_t m_shard_cnt;
    size_t m_shard_size_cap;

    std::vector<Shard*> m_shards;
    std::vector<bool> m_owns;

    InternalLevel *clone() {
        auto new_level = new InternalLevel(m_level_no, m_shards.size());
        for (size_t i=0; i<m_shard_cnt; i++) {
            new_level->m_shards[i] = m_shards[i];
            new_level->m_owns[i] = true;
            m_owns[i] = false;
        }
    }
};

}
