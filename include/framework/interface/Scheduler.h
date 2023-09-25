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
#include "framework/interface/Record.h"
#include "util/types.h"

template <typename S>
concept SchedulerInterface = requires(S s, size_t i, void *vp) {
    {S(i, i)};
//    {s.schedule_merge(vp, vp)};
    
/*
    {q.get_query_state(p, p)} -> std::convertible_to<void*>;
    {q.get_buffer_query_state(p, p)};
    {q.query(p, p)};
    {q.buffer_query(p, p)};
    {q.merge()};
    {q.delete_query_state(p)};
*/
    //{Q::get_query_state(p, p)} -> std::convertible_to<void*>;
    //{Q::get_buffer_query_state(p, p)} -> std::convertible_to<void*>;
};
