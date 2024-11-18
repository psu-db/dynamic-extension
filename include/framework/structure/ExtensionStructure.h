/*
 * include/framework/structure/ExtensionStructure.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
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

template <ShardInterface ShardType, QueryInterface<ShardType> QueryType,
          LayoutPolicy L = LayoutPolicy::TEIRING>
class ExtensionStructure {
  typedef typename ShardType::RECORD RecordType;
  typedef BufferView<RecordType> BuffView;

  typedef struct {
    size_t reccnt;
    size_t reccap;

    size_t shardcnt;
    size_t shardcap;
  } level_state;

  typedef std::vector<level_state> state_vector;

public:
  ExtensionStructure(size_t buffer_size, size_t scale_factor,
                     double max_delete_prop)
      : m_scale_factor(scale_factor), m_max_delete_prop(max_delete_prop),
        m_buffer_size(buffer_size) {}

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
  ExtensionStructure<ShardType, QueryType, L> *copy() {
    auto new_struct = new ExtensionStructure<ShardType, QueryType, L>(
        m_buffer_size, m_scale_factor, m_max_delete_prop);
    for (size_t i = 0; i < m_levels.size(); i++) {
      new_struct->m_levels.push_back(m_levels[i]->clone());
    }

    new_struct->m_refcnt = 0;
    new_struct->m_current_state = m_current_state;

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
  int tagged_delete(const RecordType &rec) {
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
    state_vector tmp = m_current_state;

    if (tmp.size() == 0) {
      grow(tmp);
    }

    assert(can_reconstruct_with(0, buffer.get_record_count(), tmp));
    flush_buffer_into_l0(std::move(buffer));

    return true;
  }

  /*
   * Return the total number of records (including tombstones) within all
   * of the levels of the structure.
   */
  size_t get_record_count() {
    size_t cnt = 0;

    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i])
        cnt += m_levels[i]->get_record_count();
    }

    return cnt;
  }

  /*
   * Return the total number of tombstones contained within all of the
   * levels of the structure.
   */
  size_t get_tombstone_count() {
    size_t cnt = 0;

    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i])
        cnt += m_levels[i]->get_tombstone_count();
    }

    return cnt;
  }

  /*
   * Return the number of levels within the structure. Note that not
   * all of these levels are necessarily populated.
   */
  size_t get_height() { return m_levels.size(); }

  /*
   * Return the amount of memory (in bytes) used by the shards within the
   * structure for storing the primary data structure and raw data.
   */
  size_t get_memory_usage() {
    size_t cnt = 0;
    for (size_t i = 0; i < m_levels.size(); i++) {
      if (m_levels[i])
        cnt += m_levels[i]->get_memory_usage();
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
    for (size_t i = 0; i < m_levels.size(); i++) {
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
    long double ts_prop = (long double)m_levels[level]->get_tombstone_count() /
                          (long double)calc_level_record_capacity(level);
    return ts_prop <= (long double)m_max_delete_prop;
  }

  /*
   * Return a reference to the underlying vector of levels within the
   * structure.
   */
  std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>> &
  get_levels() {
    return m_levels;
  }

  /*
   * NOTE: This cannot be simulated, because tombstone cancellation is not
   * cheaply predictable. It is possible that the worst case number could
   * be used instead, to allow for prediction, but compaction isn't a
   * major concern outside of sampling; at least for now. So I'm not
   * going to focus too much time on it at the moment.
   */
  ReconstructionVector get_compaction_tasks() {
    ReconstructionVector tasks;
    state_vector scratch_state = m_current_state;

    /* if the tombstone/delete invariant is satisfied, no need for compactions
     */
    if (validate_tombstone_proportion()) {
      return tasks;
    }

    /* locate the first level to violate the invariant */
    level_index violation_idx = -1;
    for (level_index i = 0; i < m_levels.size(); i++) {
      if (!validate_tombstone_proportion(i)) {
        violation_idx = i;
        break;
      }
    }

    assert(violation_idx != -1);

    level_index base_level =
        find_reconstruction_target(violation_idx, scratch_state);
    if (base_level == -1) {
      base_level = grow(scratch_state);
    }

    for (level_index i = base_level; i > 0; i--) {
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
        if (can_reconstruct_with(i, reccnt, scratch_state)) {
          reccnt += m_levels[i]->get_record_count();
        }
      }
      tasks.add_reconstruction(i - i, i, reccnt);
    }

    return tasks;
  }

  /*
   *
   */
  ReconstructionVector
  get_reconstruction_tasks(size_t buffer_reccnt,
                           state_vector scratch_state = {}) {
    /*
     * If no scratch state vector is provided, use a copy of the
     * current one. The only time an empty vector could be used as
     * *real* input to this function is when the current state is also
     * empty, so this should would even in that case.
     */
    if (scratch_state.size() == 0) {
      scratch_state = m_current_state;
    }

    ReconstructionVector reconstructions;
    size_t LOOKAHEAD = 1;
    for (size_t i = 0; i < LOOKAHEAD; i++) {
      /*
       * If L0 cannot support a direct buffer flush, figure out what
       * work must be done to free up space first. Otherwise, the
       * reconstruction vector will be initially empty.
       */
      if (!can_reconstruct_with(0, buffer_reccnt, scratch_state)) {
        auto local_recon =
            get_reconstruction_tasks_from_level(0, scratch_state);

        /*
         * for the first iteration, we need to do all of the
         * reconstructions, so use these to initially the returned
         * reconstruction list
         */
        if (i == 0) {
          reconstructions = local_recon;
          /*
           * Quick sanity test of idea: if the next reconstruction
           * would be larger than this one, steal the largest
           * task from it and run it now instead.
           */
        } else if (local_recon.get_total_reccnt() >
                   reconstructions.get_total_reccnt()) {
          auto t = local_recon.remove_reconstruction(0);
          reconstructions.add_reconstruction(t);
        }
      }

      /* simulate the buffer flush in the scratch state */
      scratch_state[0].reccnt += buffer_reccnt;
      if (L == LayoutPolicy::TEIRING || scratch_state[0].shardcnt == 0) {
        scratch_state[0].shardcnt += 1;
      }
    }

    return reconstructions;
  }

  /*
   *
   */
  ReconstructionVector
  get_reconstruction_tasks_from_level(level_index source_level,
                                      state_vector &scratch_state) {
    ReconstructionVector reconstructions;

    /*
     * Find the first level capable of sustaining a reconstruction from
     * the level above it. If no such level exists, add a new one at
     * the bottom of the structure.
     */
    level_index base_level =
        find_reconstruction_target(source_level, scratch_state);
    if (base_level == -1) {
      base_level = grow(scratch_state);
    }

    if constexpr (L == LayoutPolicy::BSM) {
      if (base_level == 0) {
        return reconstructions;
      }

      ReconstructionTask task;
      task.target = base_level;

      size_t base_reccnt = 0;
      for (level_index i = base_level; i > source_level; i--) {
        auto recon_reccnt = scratch_state[i - 1].reccnt;
        base_reccnt += recon_reccnt;
        scratch_state[i - 1].reccnt = 0;
        scratch_state[i - 1].shardcnt = 0;
        task.add_source(i - 1, recon_reccnt);
      }

      reconstructions.add_reconstruction(task);
      scratch_state[base_level].reccnt = base_reccnt;
      scratch_state[base_level].shardcnt = 1;

      return reconstructions;
    }

    /*
     * Determine the full set of reconstructions necessary to open up
     * space in the source level.
     */
    for (level_index i = base_level; i > source_level; i--) {
      size_t recon_reccnt = scratch_state[i - 1].reccnt;
      size_t base_reccnt = recon_reccnt;

      /*
       * If using Leveling, the total reconstruction size will be the
       * records in *both* base and target, because they will need to
       * be merged (assuming that target isn't empty).
       */
      if constexpr (L == LayoutPolicy::LEVELING) {
        if (can_reconstruct_with(i, base_reccnt, scratch_state)) {
          recon_reccnt += scratch_state[i].reccnt;
        }
      }
      reconstructions.add_reconstruction(i - 1, i, recon_reccnt);

      /*
       * The base level will be emptied and its records moved to
       * the target.
       */
      scratch_state[i - 1].reccnt = 0;
      scratch_state[i - 1].shardcnt = 0;

      /*
       * The target level will have the records from the base level
       * added to it, and potentially gain a shard if the LayoutPolicy
       * is tiering or the level currently lacks any shards at all.
       */
      scratch_state[i].reccnt += base_reccnt;
      if (L == LayoutPolicy::TEIRING || scratch_state[i].shardcnt == 0) {
        scratch_state[i].shardcnt += 1;
      }
    }

    return reconstructions;
  }

  inline void reconstruction(ReconstructionTask task) {
    static_assert(L == LayoutPolicy::BSM);
    std::vector<InternalLevel<ShardType, QueryType> *> levels(
        task.sources.size());
    for (size_t i = 0; i < task.sources.size(); i++) {
      levels[i] = m_levels[task.sources[i]].get();
    }

    auto new_level = InternalLevel<ShardType, QueryType>::reconstruction(
        levels, task.target);
    if (task.target >= m_levels.size()) {
      m_current_state.push_back({new_level->get_record_count(),
                                 calc_level_record_capacity(task.target), 1,
                                 1});
      m_levels.emplace_back(new_level);
    } else {
      m_current_state[task.target] = {new_level->get_record_count(),
                                      calc_level_record_capacity(task.target),
                                      1, 1};
      m_levels[task.target] = new_level;
    }

    /* remove all of the levels that have been flattened */
    for (size_t i = 0; i < task.sources.size(); i++) {
      m_levels[task.sources[i]] =
          std::shared_ptr<InternalLevel<ShardType, QueryType>>(
              new InternalLevel<ShardType, QueryType>(task.sources[i], 1));
      m_current_state[task.sources[i]] = {
          0, calc_level_record_capacity(task.target), 0, 1};
    }

    return;
  }

  /*
   * Combine incoming_level with base_level and reconstruct the shard,
   * placing it in base_level. The two levels should be sequential--i.e. no
   * levels are skipped in the reconstruction process--otherwise the
   * tombstone ordering invariant may be violated.
   */
  inline void reconstruction(level_index base_level,
                             level_index incoming_level) {
    size_t shard_capacity = (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor;

    if (base_level >= m_levels.size()) {
      m_levels.emplace_back(
          std::shared_ptr<InternalLevel<ShardType, QueryType>>(
              new InternalLevel<ShardType, QueryType>(base_level,
                                                      shard_capacity)));
      m_current_state.push_back(
          {0, calc_level_record_capacity(base_level), 0, shard_capacity});
    }

    if constexpr (L == LayoutPolicy::LEVELING) {
      /* if the base level has a shard, merge the base and incoming together to
       * make a new one */
      if (m_levels[base_level]->get_shard_count() > 0) {
        m_levels[base_level] =
            InternalLevel<ShardType, QueryType>::reconstruction(
                m_levels[base_level].get(), m_levels[incoming_level].get());
        /* otherwise, we can just move the incoming to the base */
      } else {
        m_levels[base_level] = m_levels[incoming_level];
      }

    } else {
      m_levels[base_level]->append_level(m_levels[incoming_level].get());
      m_levels[base_level]->finalize();
    }

    /* place a new, empty level where the incoming level used to be */
    m_levels[incoming_level] =
        std::shared_ptr<InternalLevel<ShardType, QueryType>>(
            new InternalLevel<ShardType, QueryType>(
                incoming_level,
                (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor));

    /*
     * Update the state vector to match the *real* state following
     * the reconstruction
     */
    m_current_state[base_level] = {m_levels[base_level]->get_record_count(),
                                   calc_level_record_capacity(base_level),
                                   m_levels[base_level]->get_shard_count(),
                                   shard_capacity};
    m_current_state[incoming_level] = {
        0, calc_level_record_capacity(incoming_level), 0, shard_capacity};
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

  size_t get_reference_count() { return m_refcnt.load(); }

  std::vector<typename QueryType::LocalQuery *>
  get_local_queries(std::vector<std::pair<ShardID, ShardType *>> &shards,
                    typename QueryType::Parameters *parms) {

    std::vector<typename QueryType::LocalQuery *> queries;

    for (auto &level : m_levels) {
      level->get_local_queries(shards, queries, parms);
    }

    return queries;
  }

private:
  size_t m_scale_factor;
  double m_max_delete_prop;
  size_t m_buffer_size;

  std::atomic<size_t> m_refcnt;

  std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>> m_levels;

  /*
   * A pair of <record_count, shard_count> for each level in the
   * structure. Record counts may be slightly inaccurate due to
   * deletes.
   */
  state_vector m_current_state;

  /*
   * Add a new level to the scratch state and return its index.
   *
   * IMPORTANT: This does _not_ add a level to the extension structure
   * anymore. This is handled by the appropriate reconstruction and flush
   * methods as needed. This function is for use in "simulated"
   * reconstructions.
   */
  inline level_index grow(state_vector &scratch_state) {
    level_index new_idx = m_levels.size();
    size_t new_shard_cap = (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor;

    scratch_state.push_back(
        {0, calc_level_record_capacity(new_idx), 0, new_shard_cap});
    return new_idx;
  }

  /*
   * Find the first level below the level indicated by idx that
   * is capable of sustaining a reconstruction and return its
   * level index. If no such level exists, returns -1. Also
   * returns -1 if idx==0, and no such level exists, to simplify
   * the logic of the first buffer flush.
   */
  inline level_index find_reconstruction_target(level_index idx,
                                                state_vector &state) {

    /*
     * this handles the very first buffer flush, when the state vector
     * is empty.
     */
    if (idx == 0 && state.size() == 0)
      return -1;

    size_t incoming_rec_cnt = state[idx].reccnt;
    for (level_index i = idx + 1; i < state.size(); i++) {
      if (can_reconstruct_with(i, incoming_rec_cnt, state)) {
        return i;
      }

      incoming_rec_cnt = state[idx].reccnt;
    }

    return -1;
  }

  inline void flush_buffer_into_l0(BuffView buffer) {
    size_t shard_capacity = (L == LayoutPolicy::LEVELING) ? 1 : m_scale_factor;

    if (m_levels.size() == 0) {
      m_levels.emplace_back(
          std::shared_ptr<InternalLevel<ShardType, QueryType>>(
              new InternalLevel<ShardType, QueryType>(0, shard_capacity)));

      m_current_state.push_back(
          {0, calc_level_record_capacity(0), 0, shard_capacity});
    }

    if constexpr (L == LayoutPolicy::LEVELING) {
      // FIXME: Kludgey implementation due to interface constraints.
      auto old_level = m_levels[0].get();
      auto temp_level = new InternalLevel<ShardType, QueryType>(0, 1);
      temp_level->append_buffer(std::move(buffer));

      if (old_level->get_shard_count() > 0) {
        m_levels[0] = InternalLevel<ShardType, QueryType>::reconstruction(
            old_level, temp_level);
        delete temp_level;
      } else {
        m_levels[0] =
            std::shared_ptr<InternalLevel<ShardType, QueryType>>(temp_level);
      }
    } else {
      m_levels[0]->append_buffer(std::move(buffer));
    }

    /* update the state vector */
    m_current_state[0].reccnt = m_levels[0]->get_record_count();
    m_current_state[0].shardcnt = m_levels[0]->get_shard_count();
  }

  /*
   * Mark a given memory level as no-longer in use by the tree. For now this
   * will just free the level. In future, this will be more complex as the
   * level may not be able to immediately be deleted, depending upon who
   * else is using it.
   */
  inline void
  mark_as_unused(std::shared_ptr<InternalLevel<ShardType, QueryType>> level) {
    level.reset();
  }

  /*
   * Assume that level "0" should be larger than the buffer. The buffer
   * itself is index -1, which should return simply the buffer capacity.
   */
  inline size_t calc_level_record_capacity(level_index idx) {
    return m_buffer_size * pow(m_scale_factor, idx + 1);
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
  inline bool can_reconstruct_with(level_index idx, size_t incoming_rec_cnt,
                                   state_vector &state) {
    if (idx >= state.size()) {
      return false;
    }

    if constexpr (L == LayoutPolicy::LEVELING) {
      return state[idx].reccnt + incoming_rec_cnt <= state[idx].reccap;
    } else if constexpr (L == LayoutPolicy::BSM) {
      return state[idx].reccnt == 0;
    } else {
      return state[idx].shardcnt < state[idx].shardcap;
    }

    /* unreachable */
    assert(true);
  }
};

} // namespace de
