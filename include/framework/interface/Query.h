/*
 * include/framework/QueryInterface.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <concepts>
#include "util/types.h"

template <typename Q>
concept QueryInterface = requires(Q q, void *p, std::vector<void*> &s) {

/*
    {q.get_query_state(p, p)} -> std::convertible_to<void*>;
    {q.get_buffer_query_state(p, p)};
    {q.query(p, p)};
    {q.buffer_query(p, p)};
    {q.merge()};
    {q.delete_query_state(p)};
*/
    {Q::EARLY_ABORT} -> std::convertible_to<bool>;
    {Q::SKIP_DELETE_FILTER} -> std::convertible_to<bool>;
    //{Q::get_query_state(p, p)} -> std::convertible_to<void*>;
    //{Q::get_buffer_query_state(p, p)} -> std::convertible_to<void*>;
    {Q::process_query_states(p, s, p)};

    {Q::delete_query_state(std::declval<void*>())} -> std::same_as<void>;
    {Q::delete_buffer_query_state(p)};

};
