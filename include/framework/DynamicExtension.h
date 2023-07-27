/*
 * include/framework/DynamicExtension.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <numeric>
#include <cstdio>
#include <vector>

#include "framework/MutableBuffer.h"
#include "framework/InternalLevel.h"
//#include "framework/ShardInterface.h"
//#include "framework/QueryInterface.h"
//#include "framework/RecordInterface.h"

//#include "shard/WIRS.h"
#include "ds/Alias.h"
#include "util/timer.h"

namespace de {

thread_local size_t sampling_attempts = 0;
thread_local size_t sampling_rejections = 0;
thread_local size_t deletion_rejections = 0;
thread_local size_t bounds_rejections = 0;
thread_local size_t tombstone_rejections = 0;
thread_local size_t buffer_rejections = 0;

/*
 * thread_local size_t various_sampling_times go here.
 */
thread_local size_t sample_range_time = 0;
thread_local size_t alias_time = 0;
thread_local size_t alias_query_time = 0;
thread_local size_t rejection_check_time = 0;
thread_local size_t buffer_sample_time = 0;
thread_local size_t memlevel_sample_time = 0;
thread_local size_t disklevel_sample_time = 0;
thread_local size_t sampling_bailouts = 0;


enum class LayoutPolicy {
    LEVELING,
    TEIRING
};

enum class DeletePolicy {
    TOMBSTONE,
    TAGGING
};

typedef ssize_t level_index;

template <typename R, typename S, typename Q, LayoutPolicy L=LayoutPolicy::TEIRING, DeletePolicy D=DeletePolicy::TAGGING>
class DynamicExtension {
    //typedef typename S<R> Shard;
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;

public:
    DynamicExtension(size_t buffer_cap, size_t scale_factor, double max_delete_prop)
        : m_scale_factor(scale_factor), m_max_delete_prop(max_delete_prop),
          m_buffer(new Buffer(buffer_cap, buffer_cap * max_delete_prop))
    { }

    ~DynamicExtension() {
        delete m_buffer;

        for (size_t i=0; i<m_levels.size(); i++) {
            delete m_levels[i];
        }
    }

    int insert(const R &rec) {
        return internal_append(rec, false);
    }

    int erase(const R &rec) {
        Buffer *buffer;

        if constexpr (D == DeletePolicy::TAGGING) {
            auto buffer = get_buffer();

            // Check the levels first. This assumes there aren't 
            // any undeleted duplicate records.
            for (auto level : m_levels) {
                if (level && level->delete_record(rec)) {
                    return 1;
                }
            }

            // the buffer will take the longest amount of time, and 
            // probably has the lowest probability of having the record,
            // so we'll check it last.
            return buffer->delete_record(rec);
        }

        return internal_append(rec, true);
    }

    std::vector<R> query(void *parms) {
        auto buffer = get_buffer();

        // Get the buffer query state
        auto buffer_state = Q::get_buffer_query_state(buffer, parms);

        // Get the shard query states
        std::vector<std::pair<ShardID, Shard*>> shards;
        std::vector<void*> states;

        for (auto &level : m_levels) {
            level->get_query_states(shards, states, parms);
        }

        Q::process_query_states(parms, states, buffer_state);

        std::vector<std::vector<R>> query_results(shards.size() + 1);

        // Execute the query for the buffer
        auto buffer_results = Q::buffer_query(buffer, buffer_state, parms);
        query_results[0] = filter_deletes(buffer_results, {-1, -1}, buffer);

        // Execute the query for each shard
        for (size_t i=0; i<shards.size(); i++) {
            auto shard_results = Q::query(shards[i].second, states[i], parms);
            query_results[i+1] = filter_deletes(shard_results, shards[i].first, buffer);
        }
        
        // Merge the results together
        auto result = Q::merge(query_results, parms);

        for (size_t i=0; i<states.size(); i++) {
            Q::delete_query_state(states[i]);
        }

        Q::delete_buffer_query_state(buffer_state);

        return result;
    }

    size_t get_record_count() {
        size_t cnt = get_buffer()->get_record_count();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_record_count();
        }

        return cnt;
    }

    size_t get_tombstone_cnt() {
        size_t cnt = get_buffer()->get_tombstone_count();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_tombstone_count();
        }

        return cnt;
    }

    size_t get_height() {
        return m_levels.size();
    }

    size_t get_memory_usage() {
        size_t cnt = m_buffer->get_memory_usage();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_memory_usage();
        }

        return cnt;
    }

    size_t get_aux_memory_usage() {
        size_t cnt = m_buffer->get_aux_memory_usage();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) {
                cnt += m_levels[i]->get_aux_memory_usage();
            }
        }

        return cnt;
    }

    bool validate_tombstone_proportion() {
        long double ts_prop;
        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) {
                ts_prop = (long double) m_levels[i]->get_tombstone_count() / (long double) calc_level_record_capacity(i);
                if (ts_prop > (long double) m_max_delete_prop) {
                    return false;
                }
            }
        }

        return true;
    }

    size_t get_buffer_capacity() {
        return m_buffer->get_capacity();
    }
    
    Shard *create_static_structure() {
        std::vector<Shard *> shards;

        if (m_levels.size() > 0) {
            for (int i=m_levels.size() - 1; i>= 0; i--) {
                if (m_levels[i]) {
                    shards.emplace_back(m_levels[i]->get_merged_shard());
                }
            }
        }

        shards.emplace_back(new S(get_buffer()));

        Shard *shards_array[shards.size()];

        size_t j = 0;
        for (size_t i=0; i<shards.size(); i++) {
            if (shards[i]) {
                shards_array[j++] = shards[i];
            }
        }

        Shard *flattened = new S(shards_array, j);

        for (auto shard : shards) {
            delete shard;
        }

        return flattened;
    }

private:
    Buffer *m_buffer;

    size_t m_scale_factor;
    double m_max_delete_prop;

    std::vector<InternalLevel<R, S, Q> *> m_levels;

    Buffer *get_buffer() {
        return m_buffer;
    }

    int internal_append(const R &rec, bool ts) {
        Buffer *buffer;
        while (!(buffer = get_buffer()))
            ;
        
        if (buffer->is_full()) {
            merge_buffer();
        }

        return buffer->append(rec, ts);
    }

    std::vector<R> filter_deletes(std::vector<Wrapped<R>> &records, ShardID shid, Buffer *buffer) {
        std::vector<R> processed_records;
        processed_records.reserve(records.size());

        // For delete tagging, we just need to check the delete bit on each
        // record.
        if constexpr (D == DeletePolicy::TAGGING) {
            for (auto &rec : records) {
                if (rec.is_deleted()) {
                    continue;
                }

                processed_records.emplace_back(rec.rec);
            }

            return processed_records;
        }

        // For tombstone deletes, we need to search for the corresponding 
        // tombstone for each record.
        for (auto &rec : records) {
           if (rec.is_tombstone()) {
                continue;
            } 

            if (buffer->check_tombstone(rec.rec)) {
                continue;
            }

            if (shid.level_idx != -1 || shid.shard_idx != -1) {
                for (size_t lvl=0; lvl<=shid.level_idx; lvl++) {
                    if (m_levels[lvl]->check_tombstone(0, rec.rec)) {
                        continue;
                    }
                }

                if (m_levels[shid.level_idx]->check_tombstone(shid.shard_idx + 1, rec.rec)) {
                    continue;
                }
            }

            processed_records.emplace_back(rec.rec);
        }

        return processed_records;
    }

    /*
     * Add a new level to the LSM Tree and return that level's index. Will
     * automatically determine whether the level should be on memory or on disk,
     * and act appropriately.
     */
    inline level_index grow() {
        level_index new_idx;

        size_t new_shard_cnt = (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor;
        new_idx = m_levels.size();
        if (new_idx > 0) {
            assert(m_levels[new_idx - 1]->get_shard(0)->get_tombstone_count() == 0);
        }
        m_levels.emplace_back(new InternalLevel<R, Shard, Q>(new_idx, new_shard_cnt));

        return new_idx;
    }


    // Merge the memory table down into the tree, completing any required other
    // merges to make room for it.
    inline void merge_buffer() {
        auto buffer = get_buffer();

        if (!can_merge_with(0, buffer->get_record_count())) {
            merge_down(0);
        }

        merge_buffer_into_l0(buffer);
        enforce_delete_maximum(0);

        buffer->truncate();
        return;
    }

    /*
     * Merge the specified level down into the tree. The level index must be
     * non-negative (i.e., this function cannot be used to merge the buffer). This
     * routine will recursively perform any necessary merges to make room for the 
     * specified level.
     */
    inline void merge_down(level_index idx) {
        level_index merge_base_level = find_mergable_level(idx);
        if (merge_base_level == -1) {
            merge_base_level = grow();
        }

        for (level_index i=merge_base_level; i>idx; i--) {
            merge_levels(i, i-1);
            enforce_delete_maximum(i);
        }

        return;
    }

    /*
     * Find the first level below the level indicated by idx that
     * is capable of sustaining a merge operation and return its
     * level index. If no such level exists, returns -1. Also
     * returns -1 if idx==0, and no such level exists, to simplify
     * the logic of the first merge.
     */
    inline level_index find_mergable_level(level_index idx, Buffer *buffer=nullptr) {

        if (idx == 0 && m_levels.size() == 0) return -1;

        bool level_found = false;
        bool disk_level;
        level_index merge_level_idx;

        size_t incoming_rec_cnt = get_level_record_count(idx, buffer);
        for (level_index i=idx+1; i<m_levels.size(); i++) {
            if (can_merge_with(i, incoming_rec_cnt)) {
                return i;
            }

            incoming_rec_cnt = get_level_record_count(i);
        }

        return -1;
    }

    /*
     * Merge the level specified by incoming level into the level specified
     * by base level. The two levels should be sequential--i.e. no levels
     * are skipped in the merge process--otherwise the tombstone ordering
     * invariant may be violated by the merge operation.
     */
    inline void merge_levels(level_index base_level, level_index incoming_level) {
        // merging two memory levels
        if constexpr (L == LayoutPolicy::LEVELING) {
            auto tmp = m_levels[base_level];
            m_levels[base_level] = InternalLevel<R, Shard, Q>::merge_levels(m_levels[base_level], m_levels[incoming_level]);
            mark_as_unused(tmp);
        } else {
            m_levels[base_level]->append_merged_shards(m_levels[incoming_level]);
        }

        mark_as_unused(m_levels[incoming_level]);
        m_levels[incoming_level] = new InternalLevel<R, Shard, Q>(incoming_level, (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor);
    }


    inline void merge_buffer_into_l0(Buffer *buffer) {
        assert(m_levels[0]);
        if constexpr (L == LayoutPolicy::LEVELING) {
            // FIXME: Kludgey implementation due to interface constraints.
            auto old_level = m_levels[0];
            auto temp_level = new InternalLevel<R, Shard, Q>(0, 1);
            temp_level->append_buffer(buffer);
            auto new_level = InternalLevel<R, Shard, Q>::merge_levels(old_level, temp_level);

            m_levels[0] = new_level;
            delete temp_level;
            mark_as_unused(old_level);
        } else {
            m_levels[0]->append_buffer(buffer);
        }
    }

    /*
     * Mark a given memory level as no-longer in use by the tree. For now this
     * will just free the level. In future, this will be more complex as the
     * level may not be able to immediately be deleted, depending upon who
     * else is using it.
     */ 
    inline void mark_as_unused(InternalLevel<R, Shard, Q> *level) {
        delete level;
    }

    /*
     * Check the tombstone proportion for the specified level and
     * if the limit is exceeded, forcibly merge levels until all
     * levels below idx are below the limit.
     */
    inline void enforce_delete_maximum(level_index idx) {
        long double ts_prop = (long double) m_levels[idx]->get_tombstone_count() / (long double) calc_level_record_capacity(idx);

        if (ts_prop > (long double) m_max_delete_prop) {
            merge_down(idx);
        }

        return;
    }

    /*
     * Assume that level "0" should be larger than the buffer. The buffer
     * itself is index -1, which should return simply the buffer capacity.
     */
    inline size_t calc_level_record_capacity(level_index idx) {
        return get_buffer()->get_capacity() * pow(m_scale_factor, idx+1);
    }

    /*
     * Returns the actual number of records present on a specified level. An
     * index value of -1 indicates the memory table. Can optionally pass in
     * a pointer to the memory table to use, if desired. Otherwise, there are
     * no guarantees about which buffer will be accessed if level_index is -1.
     */
    inline size_t get_level_record_count(level_index idx, Buffer *buffer=nullptr) {

        assert(idx >= -1);
        if (idx == -1) {
            return (buffer) ? buffer->get_record_count() : get_buffer()->get_record_count();
        }

        return (m_levels[idx]) ? m_levels[idx]->get_record_count() : 0;
    }

    /*
     * Determines if the specific level can merge with another record containing
     * incoming_rec_cnt number of records. The provided level index should be 
     * non-negative (i.e., not refer to the buffer) and will be automatically
     * translated into the appropriate index into either the disk or memory level
     * vector.
     */
    inline bool can_merge_with(level_index idx, size_t incoming_rec_cnt) {
        if (idx>= m_levels.size() || !m_levels[idx]) {
            return false;
        }

        if (L == LayoutPolicy::LEVELING) {
            return m_levels[idx]->get_record_count() + incoming_rec_cnt <= calc_level_record_capacity(idx);
        } else {
            return m_levels[idx]->get_shard_count() < m_scale_factor;
        }

        // unreachable
        assert(true);
    }
};

}

