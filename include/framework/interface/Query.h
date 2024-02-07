/*
 * include/framework/interface/Query.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/QueryRequirements.h"
#include <concepts>

namespace de{
// FIXME: The interface is not completely specified yet, as it is pending
//        determining a good way to handle additional template arguments 
//        to get the Shard and Record types into play
template <typename Q, typename R, typename S>
concept QueryInterface = requires(void *p, S *sh, std::vector<void*> &s, std::vector<std::vector<Wrapped<R>>> &rv, BufferView<R> *bv) {
    {Q::get_query_state(sh, p)} -> std::convertible_to<void*>;
    {Q::get_buffer_query_state(bv, p)} -> std::convertible_to<void *>;
    {Q::process_query_states(p, s, p)};
    {Q::query(sh, p, p)} -> std::convertible_to<std::vector<Wrapped<R>>>;
    {Q::buffer_query(p, p)} -> std::convertible_to<std::vector<Wrapped<R>>>;
    {Q::merge(rv, p)} -> std::convertible_to<std::vector<R>>;

    {Q::delete_query_state(p)} -> std::same_as<void>;
    {Q::delete_buffer_query_state(p)} -> std::same_as<void>;

    {Q::EARLY_ABORT} -> std::convertible_to<bool>;
    {Q::SKIP_DELETE_FILTER} -> std::convertible_to<bool>;
};
}
