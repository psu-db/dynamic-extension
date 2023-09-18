/*
 * include/framework/ExtensionStructure.h
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
#include "framework/ShardInterface.h"
#include "framework/QueryInterface.h"
#include "framework/RecordInterface.h"

#include "framework/Configuration.h"

#include "psu-util/timer.h"
#include "psu-ds/Alias.h"

namespace de {

struct MergeTask {
    level_index m_source_level;
    level_index m_target_level;
    size_t m_size;
    size_t m_timestamp;

    bool operator<(MergeTask &other) {
        return m_timestamp < other.m_timestamp;
    }
};

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L=LayoutPolicy::TEIRING>
class ExtensionStructure {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;

public:
    ExtensionStructure(size_t buffer_size, size_t scale_factor, double max_delete_prop)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(max_delete_prop)
        , m_buffer_size(buffer_size)
    {}

    ~ExtensionStructure() = default;

    /*
     * Create a shallow copy of this extension structure. The copy will share references to the
     * same levels/shards as the original, but will have its own lists. As all of the shards are
     * immutable (with the exception of deletes), the copy can be restructured with merges, etc., 
     * without affecting the original.
     *
     * NOTE: When using tagged deletes, a delete of a record in the original structure will affect
     * the copy, so long as the copy retains a reference to the same shard as the original. This could
     * cause synchronization problems under tagging with concurrency. Any deletes in this context will
     * need to be forwarded to the appropriate structures manually.
     */
    ExtensionStructure<R, S, Q, L> *copy() {
       auto new_struct = new ExtensionStructure<R, S, Q, L>(m_scale_factor, m_max_delete_prop, m_buffer_size);
        for (size_t i=0; i<m_levels.size(); i++) {
            new_struct->m_levels.push_back(m_levels[i]);
        }

        return new_struct;
    }

    /*
     * Search for a record matching the argument and mark it deleted by
     * setting the delete bit in its wrapped header. Returns 1 if a matching
     * record was found and deleted, and 0 if a matching record was not found.
     *
     * This function will stop after finding the first matching record. It is assumed
     * that no duplicate records exist. In the case of duplicates, this function will
     * still "work", but in the sense of "delete first match".
     */
    int tagged_delete(const R &rec) {
        for (auto level : m_levels) {
            if (level && level->delete_record(rec)) {
                return 1;
            }
        }

        /*
         * If the record to be erased wasn't found, return 0. The
         * DynamicExtension itself will then search the active 
         * Buffers.
         */
        return 0;
    }

    /*
     * Merge the memory table down into the tree, completing any required other
     * merges to make room for it.
     */
    inline bool merge_buffer(Buffer *buffer) {
        assert(can_merge_with(0, buffer->get_record_count()));

        merge_buffer_into_l0(buffer);
        enforce_delete_maximum(0);

        buffer->truncate();
        return true;
    }

    /*
     * Return the total number of records (including tombstones) within all
     * of the levels of the structure.
     */
    size_t get_record_count() {
        size_t cnt = 0;

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_record_count();
        }

        return cnt;
    }

    /*
     * Return the total number of tombstones contained within all of the
     * levels of the structure.
     */
    size_t get_tombstone_cnt() {
        size_t cnt = 0;

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_tombstone_count();
        }

        return cnt;
    }

    /*
     * Return the number of levels within the structure. Note that not
     * all of these levels are necessarily populated.
     */
    size_t get_height() {
        return m_levels.size();
    }

    /*
     * Return the amount of memory (in bytes) used by the shards within the
     * structure for storing the primary data structure and raw data.
     */
    size_t get_memory_usage() {
        size_t cnt = 0;
        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_memory_usage();
        }

        return cnt;
    }

    /*
     * Return the amount of memory (in bytes) used by the shards within the
     * structure for storing auxiliary data structures. This total does not
     * include memory used for the main data structure, or raw data.
     */
    size_t get_aux_memory_usage() {
        size_t cnt = 0;
        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) {
                cnt += m_levels[i]->get_aux_memory_usage();
            }
        }

        return cnt;
    }

    /*
     * Validate that no level in the structure exceeds its maximum tombstone capacity. This is
     * used to trigger preemptive compactions at the end of the merge process.
     */
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

    /*
     * Return a reference to the underlying vector of levels within the
     * structure.
     */
    std::vector<std::shared_ptr<InternalLevel<R, S, Q>>> &get_levels() {
        return m_levels;
    }

    /*
     *
     */
    std::vector<MergeTask> get_merge_tasks(size_t buffer_reccnt) {
        std::vector<MergeTask> merges;

        /*
         * The buffer -> L0 merge task is not included so if that 
         * can be done without any other change, just return an 
         * empty list.
         */
        if (can_merge_with(0, buffer_reccnt)) {
            return std::move(merges); 
        }

        level_index merge_base_level = find_mergable_level(0);
        if (merge_base_level == -1) {
            merge_base_level = grow();
        }

        for (level_index i=merge_base_level; i>0; i--) {
            MergeTask task;
            task.m_source_level = i - 1;
            task.m_target_level = i;

            /*
             * The amount of storage required for the merge accounts
             * for the cost of storing the new records, along with the
             * cost of retaining the old records during the process 
             * (hence the 2x multiplier). 
             *
             * FIXME: currently does not account for the *actual* size 
             * of the shards, only the storage for the records 
             * themselves.
             */
            size_t reccnt = m_levels[i-1]->get_record_count();
            if constexpr (L == LayoutPolicy::LEVELING) {
                if (can_merge_with(i, reccnt)) {
                    reccnt += m_levels[i]->get_record_count();
                }
            }
            task.m_size = 2* reccnt * sizeof(R);

            merges.push_back(task);
        }

        return std::move(merges);
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
            m_levels[base_level] = InternalLevel<R, Shard, Q>::merge_levels(m_levels[base_level].get(), m_levels[incoming_level].get());
        } else {
            m_levels[base_level]->append_merged_shards(m_levels[incoming_level].get());
        }

        m_levels[incoming_level] = std::shared_ptr<InternalLevel<R, Shard, Q>>(new InternalLevel<R, Shard, Q>(incoming_level, (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor));
    }


private:
    size_t m_scale_factor;
    double m_max_delete_prop;
    size_t m_buffer_size;

    std::vector<std::shared_ptr<InternalLevel<R, S, Q>>> m_levels;

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
        m_levels.emplace_back(std::shared_ptr<InternalLevel<R, Shard, Q>>(new InternalLevel<R, Shard, Q>(new_idx, new_shard_cnt)));

        return new_idx;
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


    inline void merge_buffer_into_l0(Buffer *buffer) {
        assert(m_levels[0]);
        if constexpr (L == LayoutPolicy::LEVELING) {
            // FIXME: Kludgey implementation due to interface constraints.
            auto old_level = m_levels[0].get();
            auto temp_level = new InternalLevel<R, Shard, Q>(0, 1);
            temp_level->append_buffer(buffer);
            auto new_level = InternalLevel<R, Shard, Q>::merge_levels(old_level, temp_level);

            m_levels[0] = new_level;
            delete temp_level;
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
    inline void mark_as_unused(std::shared_ptr<InternalLevel<R, Shard, Q>> level) {
        level.reset();
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
        return m_buffer_size * pow(m_scale_factor, idx+1);
    }

    /*
     * Returns the actual number of records present on a specified level. An
     * index value of -1 indicates the memory table. Can optionally pass in
     * a pointer to the memory table to use, if desired. Otherwise, there are
     * no guarantees about which buffer will be accessed if level_index is -1.
     */
    inline size_t get_level_record_count(level_index idx, Buffer *buffer=nullptr) {
        if (buffer) { 
            return buffer->get_record_count();
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

