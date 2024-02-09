/*
 * include/framework/structure/ExtensionStructure.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <cstdio>
#include <vector>

#include "framework/structure/BufferView.h"
#include "framework/structure/InternalLevel.h"

#include "framework/util/Configuration.h"

#include "psu-util/timer.h"

namespace de {

template <RecordInterface R, ShardInterface<R> S, QueryInterface<R, S> Q, LayoutPolicy L=LayoutPolicy::TEIRING>
class ExtensionStructure {
    typedef S Shard;
    typedef BufferView<R> BuffView;

public:
    ExtensionStructure(size_t buffer_size, size_t scale_factor, double max_delete_prop)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(max_delete_prop)
        , m_buffer_size(buffer_size)
    {}

    ~ExtensionStructure() = default;

    /*
     * Create a shallow copy of this extension structure. The copy will share
     * references to the same levels/shards as the original, but will have its
     * own lists. As all of the shards are immutable (with the exception of
     * deletes), the copy can be restructured with reconstructions and flushes
     * without affecting the original. The copied structure will be returned
     * with a reference count of 0; generally you will want to immediately call
     * take_reference() on it.
     *
     * NOTE: When using tagged deletes, a delete of a record in the original
     * structure will affect the copy, so long as the copy retains a reference
     * to the same shard as the original. This could cause synchronization
     * problems under tagging with concurrency. Any deletes in this context will
     * need to be forwarded to the appropriate structures manually.
     */
    ExtensionStructure<R, S, Q, L> *copy() {
        auto new_struct = new ExtensionStructure<R, S, Q, L>(m_buffer_size, m_scale_factor, 
                                                             m_max_delete_prop);
        for (size_t i=0; i<m_levels.size(); i++) {
            new_struct->m_levels.push_back(m_levels[i]->clone());
        }

        new_struct->m_refcnt = 0;

        return new_struct;
    }

    /*
     * Search for a record matching the argument and mark it deleted by
     * setting the delete bit in its wrapped header. Returns 1 if a matching
     * record was found and deleted, and 0 if a matching record was not found.
     *
     * This function will stop after finding the first matching record. It is
     * assumed that no duplicate records exist. In the case of duplicates, this
     * function will still "work", but in the sense of "delete first match".
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
     * Flush a buffer into the extension structure, performing any necessary
     * reconstructions to free up room in L0.
     *
     * FIXME: arguably, this should be a method attached to the buffer that
     * takes a structure as input.
     */
    inline bool flush_buffer(BuffView buffer) {
        assert(can_reconstruct_with(0, buffer.get_record_count()));

        flush_buffer_into_l0(std::move(buffer));

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
    size_t get_tombstone_count() {
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
     * Validate that no level in the structure exceeds its maximum tombstone
     * capacity. This is used to trigger preemptive compactions at the end of
     * the reconstruction process.
     */
    bool validate_tombstone_proportion() {
      long double ts_prop;
      for (size_t i = 0; i < m_levels.size(); i++) {
        if (m_levels[i]) {
          ts_prop = (long double)m_levels[i]->get_tombstone_count() /
                    (long double)calc_level_record_capacity(i);
          if (ts_prop > (long double)m_max_delete_prop) {
            return false;
          }
        }
      }

      return true;
    }

    bool validate_tombstone_proportion(level_index level) {
        long double ts_prop =  (long double) m_levels[level]->get_tombstone_count() / (long double) calc_level_record_capacity(level);
        return ts_prop <= (long double) m_max_delete_prop;
    }

    /*
     * Return a reference to the underlying vector of levels within the
     * structure.
     */
    std::vector<std::shared_ptr<InternalLevel<R, S, Q>>> &get_levels() {
        return m_levels;
    }

    std::vector<ReconstructionTask> get_compaction_tasks() {
        std::vector<ReconstructionTask> tasks;

        /* if the tombstone/delete invariant is satisfied, no need for compactions */
        if (validate_tombstone_proportion()) {
            return tasks;
        }

        /* locate the first level to violate the invariant */
        level_index violation_idx = -1;
        for (level_index i=0; i<m_levels.size(); i++) {
            if (!validate_tombstone_proportion(i))  {
                violation_idx = i;
                break;
            }
        }

        assert(violation_idx != -1);

        level_index base_level = find_reconstruction_target(violation_idx);
        if (base_level == -1) {
            base_level = grow();
        }

        for (level_index i=base_level; i>0; i--) {
            ReconstructionTask task = {i-1, i};

            /*
             * The amount of storage required for the reconstruction accounts
             * for the cost of storing the new records, along with the
             * cost of retaining the old records during the process
             * (hence the 2x multiplier).
             *
             * FIXME: currently does not account for the *actual* size
             * of the shards, only the storage for the records
             * themselves.
             */
            size_t reccnt = m_levels[i - 1]->get_record_count();
            if constexpr (L == LayoutPolicy::LEVELING) {
                if (can_reconstruct_with(i, reccnt)) {
                    reccnt += m_levels[i]->get_record_count();
                }
            }
            //task.m_size = 2* reccnt * sizeof(R);

            tasks.push_back(task);
        }

        return tasks;
    }

    /*
     *
     */
    std::vector<ReconstructionTask> get_reconstruction_tasks(size_t buffer_reccnt) {
        std::vector<ReconstructionTask> reconstructions;

        /*
         * The buffer flush is not included so if that can be done without any
         * other change, just return an empty list.
         */
        if (can_reconstruct_with(0, buffer_reccnt)) {
            return std::move(reconstructions); 
        }

        level_index base_level = find_reconstruction_target(0);
        if (base_level == -1) {
            base_level = grow();
        }

        for (level_index i=base_level; i>0; i--) {
            ReconstructionTask task = {i-1, i};

            /*
             * The amount of storage required for the reconstruction accounts
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
                if (can_reconstruct_with(i, reccnt)) {
                    reccnt += m_levels[i]->get_record_count();
                }
            }
            //task.m_size = 2* reccnt * sizeof(R);

            reconstructions.push_back(task);
        }

        return std::move(reconstructions);
    }


    /*
     *
     */
    std::vector<ReconstructionTask> get_reconstruction_tasks_from_level(level_index source_level) {
        std::vector<ReconstructionTask> reconstructions;

        level_index base_level = find_reconstruction_target(source_level);
        if (base_level == -1) {
            base_level = grow();
        }

        for (level_index i=base_level; i>source_level; i--) {
            ReconstructionTask task = {i - 1, i};
            /*
             * The amount of storage required for the reconstruction accounts
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
                if (can_reconstruct_with(i, reccnt)) {
                    reccnt += m_levels[i]->get_record_count();
                }
            }
//            task.m_size = 2* reccnt * sizeof(R);

            reconstructions.push_back(task);
        }

        return reconstructions;
    }

    /*
     * Combine incoming_level with base_level and reconstruct the shard,
     * placing it in base_level. The two levels should be sequential--i.e. no
     * levels are skipped in the reconstruction process--otherwise the
     * tombstone ordering invariant may be violated.
     */
    inline void reconstruction(level_index base_level, level_index incoming_level) {
        if constexpr (L == LayoutPolicy::LEVELING) {
            /* if the base level has a shard, merge the base and incoming together to make a new one */
            if (m_levels[base_level]->get_shard_count() > 0) {
                m_levels[base_level] = InternalLevel<R, Shard, Q>::reconstruction(m_levels[base_level].get(), m_levels[incoming_level].get());
            /* otherwise, we can just move the incoming to the base */
            } else {
                m_levels[base_level] = m_levels[incoming_level];
            }
        } else {
            m_levels[base_level]->append_level(m_levels[incoming_level].get());
            m_levels[base_level]->finalize();
        }

        /* place a new, empty level where the incoming level used to be */
        m_levels[incoming_level] = std::shared_ptr<InternalLevel<R, Shard, Q>>(new InternalLevel<R, Shard, Q>(incoming_level, (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor));
    }

    bool take_reference() {
        m_refcnt.fetch_add(1);
        return true;
    }

    bool release_reference() {
        assert(m_refcnt.load() > 0);
        m_refcnt.fetch_add(-1);
        return true;
    }

    size_t get_reference_count() {
        return m_refcnt.load();
    }

    std::vector<void *> get_query_states(std::vector<std::pair<ShardID, Shard*>> &shards, void *parms) {
        std::vector<void*> states;

        for (auto &level : m_levels) {
            level->get_query_states(shards, states, parms);
        }

        return states;
    }

private:
    size_t m_scale_factor;
    double m_max_delete_prop;
    size_t m_buffer_size;

    std::atomic<size_t> m_refcnt;

    std::vector<std::shared_ptr<InternalLevel<R, S, Q>>> m_levels;

    /*
     * Add a new level to the structure and return its index.
     */
    inline level_index grow() {
        level_index new_idx = m_levels.size();
        size_t new_shard_cnt = (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor;

        m_levels.emplace_back(std::shared_ptr<InternalLevel<R, Shard, Q>>(new InternalLevel<R, Shard, Q>(new_idx, new_shard_cnt)));
        return new_idx;
    }

    /*
     * Find the first level below the level indicated by idx that
     * is capable of sustaining a reconstruction and return its
     * level index. If no such level exists, returns -1. Also
     * returns -1 if idx==0, and no such level exists, to simplify
     * the logic of the first buffer flush.
     */
    inline level_index find_reconstruction_target(level_index idx) {

        if (idx == 0 && m_levels.size() == 0) return -1;

        size_t incoming_rec_cnt = get_level_record_count(idx);
        for (level_index i=idx+1; i<m_levels.size(); i++) {
            if (can_reconstruct_with(i, incoming_rec_cnt)) {
                return i;
            }

            incoming_rec_cnt = get_level_record_count(i);
        }

        return -1;
    }

    inline void flush_buffer_into_l0(BuffView buffer) {
        assert(m_levels[0]);
        if constexpr (L == LayoutPolicy::LEVELING) {
            // FIXME: Kludgey implementation due to interface constraints.
            auto old_level = m_levels[0].get();
            auto temp_level = new InternalLevel<R, Shard, Q>(0, 1);
            temp_level->append_buffer(std::move(buffer));

            if (old_level->get_shard_count() > 0) {
                m_levels[0] = InternalLevel<R, Shard, Q>::reconstruction(old_level, temp_level);
                delete temp_level;
            } else {
                m_levels[0] = std::shared_ptr<InternalLevel<R, Shard, Q>>(temp_level);
            }
        } else {
            m_levels[0]->append_buffer(std::move(buffer));
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
     * Assume that level "0" should be larger than the buffer. The buffer
     * itself is index -1, which should return simply the buffer capacity.
     */
    inline size_t calc_level_record_capacity(level_index idx) {
        return m_buffer_size * pow(m_scale_factor, idx+1);
    }

    /*
     * Returns the number of records present on a specified level. 
     */
    inline size_t get_level_record_count(level_index idx) {
        return (m_levels[idx]) ? m_levels[idx]->get_record_count() : 0;
    }

    /*
     * Determines if a level can sustain a reconstruction with incoming_rec_cnt
     * additional records without exceeding its capacity.
     */
    inline bool can_reconstruct_with(level_index idx, size_t incoming_rec_cnt) {
        if (idx >= m_levels.size() || !m_levels[idx]) {
            return false;
        }

        if (L == LayoutPolicy::LEVELING) {
            return m_levels[idx]->get_record_count() + incoming_rec_cnt <= calc_level_record_capacity(idx);
        } else {
            return m_levels[idx]->get_shard_count() < m_scale_factor;
        }

        /* unreachable */
        assert(true);
    }
};

}

