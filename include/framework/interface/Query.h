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

namespace de{

template <typename Q, typename R, typename S>
concept QueryInterface = requires(void *p, S *sh, std::vector<void*> &s, std::vector<std::vector<Wrapped<R>>> &rv, BufferView<R> *bv, std::vector<R> &resv) {
    {Q::get_query_state(sh, p)} -> std::convertible_to<void*>;
    {Q::get_buffer_query_state(bv, p)} -> std::convertible_to<void *>;
    {Q::process_query_states(p, s, p)};
    {Q::query(sh, p, p)} -> std::convertible_to<std::vector<Wrapped<R>>>;
    {Q::buffer_query(p, p)} -> std::convertible_to<std::vector<Wrapped<R>>>;
    {Q::merge(rv, p, resv)};

    {Q::delete_query_state(p)} -> std::same_as<void>;
    {Q::delete_buffer_query_state(p)} -> std::same_as<void>;

    {Q::repeat(p, resv, s, p)} -> std::same_as<bool>;

    {Q::EARLY_ABORT} -> std::convertible_to<bool>;
    {Q::SKIP_DELETE_FILTER} -> std::convertible_to<bool>;
};
}
