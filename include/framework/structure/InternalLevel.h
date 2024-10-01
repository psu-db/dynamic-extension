/*
 * include/framework/structure/InternalLevel.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * The word `Internal` in this class's name refers to memory. The current
 * model, inherited from the framework in Practical Dynamic Extension for
 * Sampling Indexes, would use a different ExternalLevel for shards stored
 * on external storage. This is a distinction that can probably be avoided
 * with some more thought being put into interface design.
 *
 */
#pragma once

#include <vector>
#include <memory>

#include "util/types.h"
#include "framework/interface/Shard.h"
#include "framework/interface/Query.h"
#include "framework/interface/Record.h"
#include "framework/structure/BufferView.h"

namespace de {
template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class InternalLevel;



template <ShardInterface ShardType, QueryInterface<ShardType> QueryType>
class InternalLevel {
    typedef typename ShardType::RECORD RecordType;
    typedef BufferView<RecordType> BuffView;
public:
    InternalLevel(ssize_t level_no, size_t shard_cap)
    : m_level_no(level_no)
    , m_shard_cnt(0)
    , m_shards(shard_cap, nullptr)
    , m_pending_shard(nullptr)
    {}

    ~InternalLevel() { 
        delete m_pending_shard;
    }

    /*
     * Create a new shard combining the records from base_level and new_level,
     * and return a shared_ptr to a new level containing this shard. This is used
     * for reconstructions under the leveling layout policy.
     *
     * No changes are made to the levels provided as arguments.
     */
    static std::shared_ptr<InternalLevel> reconstruction(InternalLevel* base_level, InternalLevel* new_level) {
        assert(base_level->m_level_no > new_level->m_level_no || (base_level->m_level_no == 0 && new_level->m_level_no == 0));
        auto res = new InternalLevel(base_level->m_level_no, 1);
        res->m_shard_cnt = 1;
        std::vector<ShardType *> shards = {base_level->m_shards[0].get(),
                                       new_level->m_shards[0].get()};

        res->m_shards[0] = std::make_shared<ShardType>(shards);
        return std::shared_ptr<InternalLevel>(res);
    }

    static std::shared_ptr<InternalLevel> reconstruction(std::vector<InternalLevel*> levels, size_t level_idx) {
        std::vector<ShardType *> shards; 
        for (auto level : levels) {
            for (auto shard : level->m_shards) {
                if (shard) shards.emplace_back(shard.get());
            }
        }

        auto res = new InternalLevel(level_idx, 1);
        res->m_shard_cnt = 1;
        res->m_shards[0] = std::make_shared<ShardType>(shards);

        return std::shared_ptr<InternalLevel>(res);
    }

    /*
     * Create a new shard combining the records from all of
     * the shards in level, and append this new shard into
     * this level. This is used for reconstructions under
     * the tiering layout policy.
     *
     * No changes are made to the level provided as an argument.
     */
    void append_level(InternalLevel* level) {
        // FIXME: that this is happening probably means that
        // something is going terribly wrong earlier in the
        // reconstruction logic.
        if (level->get_shard_count() == 0) {
            return;
        }

        std::vector<ShardType*> shards;
        for (auto shard : level->m_shards) {
            if (shard) shards.emplace_back(shard.get());
        }

        if (m_shard_cnt == m_shards.size()) {
            m_pending_shard = new ShardType(shards);
            return;
        }

        auto tmp = new ShardType(shards);
        m_shards[m_shard_cnt] = std::shared_ptr<ShardType>(tmp);

        ++m_shard_cnt;
    }

    /*
     * Create a new shard using the records in the
     * provided buffer, and append this new shard
     * into this level. This is used for buffer
     * flushes under the tiering layout policy.
     */
    void append_buffer(BuffView buffer) {
        if (m_shard_cnt == m_shards.size()) {
            assert(m_pending_shard == nullptr);
            m_pending_shard = new ShardType(std::move(buffer));
            return;
        }

        m_shards[m_shard_cnt] = std::make_shared<ShardType>(std::move(buffer));
        ++m_shard_cnt;
    }

    void finalize() {
        if (m_pending_shard) {
            for (size_t i=0; i<m_shards.size(); i++) {
                m_shards[i] = nullptr;
            }

            m_shards[0] = std::shared_ptr<ShardType>(m_pending_shard);
            m_pending_shard = nullptr;
            m_shard_cnt = 1;
        }
    }

    /*
     * Create a new shard containing the combined records
     * from all shards on this level and return it.
     *
     * No changes are made to this level.
     */
    ShardType *get_combined_shard() {
        if (m_shard_cnt == 0) {
            return nullptr;
        }

        std::vector<ShardType *> shards;
        for (auto shard : m_shards) {
            if (shard) shards.emplace_back(shard.get());
        }

        return new ShardType(shards);
    }

    void get_local_queries(std::vector<std::pair<ShardID, ShardType *>> &shards, 
                           std::vector<typename QueryType::LocalQuery *>& local_queries, 
                           typename QueryType::Parameters *query_parms) {
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_shards[i]) {
                auto local_query = QueryType::local_preproc(m_shards[i].get(), query_parms);
                shards.push_back({{m_level_no, (ssize_t) i}, m_shards[i].get()});
                local_queries.emplace_back(local_query);
            }
        }
    }

    bool check_tombstone(size_t shard_stop, const RecordType& rec) {
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

    bool delete_record(const RecordType &rec) {
        if (m_shard_cnt == 0) return false;

        for (size_t i = 0; i < m_shards.size();  ++i) {
            if (m_shards[i]) {
                auto res = m_shards[i]->point_lookup(rec);
                if (res) {
                    res->set_delete();
                    return true;
                }
            }
        }

        return false;
    }

    ShardType* get_shard(size_t idx) {
        if (idx >= m_shard_cnt) {
            return nullptr;
        }

        return m_shards[idx].get();
    }

    size_t get_shard_count() {
        return m_shard_cnt;
    }

    size_t get_record_count() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_shards[i]) {
                cnt += m_shards[i]->get_record_count();
            }
        }

        return cnt;
    }
    
    size_t get_tombstone_count() {
        size_t res = 0;
        for (size_t i = 0; i < m_shard_cnt; ++i) {
            if (m_shards[i]) {
                res += m_shards[i]->get_tombstone_count();
            }
        }
        return res;
    }

    size_t get_aux_memory_usage() {
        size_t cnt = 0;
        for (size_t i=0; i<m_shard_cnt; i++) {
            if (m_shards[i]){
                cnt += m_shards[i]->get_aux_memory_usage();
            }
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
                reccnt += m_shards[i]->get_record_count();
            }
        }

        return (double) tscnt / (double) (tscnt + reccnt);
    }

    std::shared_ptr<InternalLevel> clone() {
        auto new_level = std::make_shared<InternalLevel>(m_level_no, m_shards.size());
        for (size_t i=0; i<m_shard_cnt; i++) {
            new_level->m_shards[i] = m_shards[i];
        }
        new_level->m_shard_cnt = m_shard_cnt;

        return new_level;
    }

private:
    ssize_t m_level_no;
    
    size_t m_shard_cnt;
    size_t m_shard_size_cap;

    std::vector<std::shared_ptr<ShardType>> m_shards;
    ShardType *m_pending_shard;
};

}
