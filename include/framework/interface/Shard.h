/*
 * include/framework/interface/Shard.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <concepts>

#include "util/types.h"
#include "framework/interface/Record.h"

namespace de {

// FIXME: The interface is not completely specified yet, as it is pending
//        determining a good way to handle additional template arguments 
//        to get the Record type into play
template <typename S>
concept ShardInterface = requires(S s, S **spp, void *p, bool b, size_t i) {
    {S(spp, i)};
    /*
    {S(mutable buffer)}
    {s.point_lookup(r, b) } -> std::convertible_to<void*>
    */
    {s.get_data()} -> std::convertible_to<void*>;

    {s.get_record_count()} -> std::convertible_to<size_t>;
    {s.get_tombstone_count()} -> std::convertible_to<size_t>;
    {s.get_memory_usage()} -> std::convertible_to<size_t>;
    {s.get_aux_memory_usage()} -> std::convertible_to<size_t>;
};

}
