/*
 * include/framework/interface/Shard.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/ShardRequirements.h"

namespace de {

template <typename SHARD>
concept ShardInterface = RecordInterface<typename SHARD::RECORD> &&
    requires(SHARD shard, const std::vector<SHARD *> &shard_vector, bool b,
             BufferView<typename SHARD::RECORD> bv,
             typename SHARD::RECORD rec) {
  /* construct a shard from a vector of shards of the same type */
  {SHARD(shard_vector)};

  /* construct a shard from a buffer view (i.e., unsorted array of records) */
  {SHARD(std::move(bv))};

  /* perform a lookup for a record matching rec and return a pointer to it */
  {
    shard.point_lookup(rec, b)
    } -> std::same_as<Wrapped<typename SHARD::RECORD> *>;

  /*
   * return the number of records in the shard -- used to determine when
   * reconstructions occur
   */
  { shard.get_record_count() } -> std::convertible_to<size_t>;

  /*
   * return the number of tombstones in the shard -- can simply return
   * 0 if tombstones are not in use.
   */
  { shard.get_tombstone_count() } -> std::convertible_to<size_t>;

  /*
   * return the number of bytes of memory used by the main data structure
   * within the shard -- informational use only at the moment
   */
  { shard.get_memory_usage() } -> std::convertible_to<size_t>;

  /*
   * return the number of bytes of memory used by auxilliary data
   * structures (bloom filters, etc.) within the shard -- informational
   * use only at the moment
   */
  { shard.get_aux_memory_usage() } -> std::convertible_to<size_t>;

};

template <typename SHARD>
concept SortedShardInterface = ShardInterface<SHARD> &&
    requires(SHARD shard, typename SHARD::RECORD rec, size_t index) {
  { shard.lower_bound(rec) } -> std::convertible_to<size_t>;
  { shard.upper_bound(rec) } -> std::convertible_to<size_t>;
  {
    shard.get_record_at(index)
    } -> std::same_as<Wrapped<typename SHARD::RECORD> *>;
};

} // namespace de
