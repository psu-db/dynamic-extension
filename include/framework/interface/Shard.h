/*
 * include/framework/interface/Shard.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/ShardRequirements.h"

namespace de {

template <typename S, typename R>
concept ShardInterface = RecordInterface<R> && requires(S s, std::vector<S*> spp, void *p, bool b, size_t i, BufferView<R> bv, R r) {
    {S(spp)};
    {S(std::move(bv))};

    {s.point_lookup(r, b) } -> std::same_as<Wrapped<R>*>;
    {s.get_data()} -> std::same_as<Wrapped<R>*>;

    {s.get_record_count()} -> std::convertible_to<size_t>;
    {s.get_tombstone_count()} -> std::convertible_to<size_t>;
    {s.get_memory_usage()} -> std::convertible_to<size_t>;
    {s.get_aux_memory_usage()} -> std::convertible_to<size_t>;
};

template <typename S, typename R>
concept SortedShardInterface = ShardInterface<S, R> && requires(S s, R r, R *rp, size_t i) {
    {s.lower_bound(r)} -> std::convertible_to<size_t>;
    {s.upper_bound(r)} -> std::convertible_to<size_t>;
    {s.get_record_at(i)} -> std::same_as<Wrapped<R>*>;
};

}
