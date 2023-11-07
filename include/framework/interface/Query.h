/*
 * include/framework/interface/Query.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <concepts>

#include "util/types.h"

// FIXME: The interface is not completely specified yet, as it is pending
//        determining a good way to handle additional template arguments 
//        to get the Shard and Record types into play
template <typename Q>
concept QueryInterface = requires(Q q, void *p, std::vector<void*> &s) {

    /*
    {Q::get_query_state(p, p)} -> std::convertible_to<void*>;
    {Q::get_buffer_query_state(p, p)} -> std::convertible_to<void *>;
    */
    {Q::process_query_states(p, s, s)};
    /*
    {Q::query(s, p, p)} -> std::convertible_to<std::vector<Wrapped<R>>>;
    {Q::buffer_query(p, p)} -> std::convertible_to<std::vector<Wrapped<R>>>;
    {Q::merge(rv, p)} -> std::convertible_to<std::vector<R>>;
    */

    {Q::delete_query_state(std::declval<void*>())} -> std::same_as<void>;
    {Q::delete_buffer_query_state(std::declval<void*>())} -> std::same_as<void>;

    {Q::EARLY_ABORT} -> std::convertible_to<bool>;
    {Q::SKIP_DELETE_FILTER} -> std::convertible_to<bool>;
};
