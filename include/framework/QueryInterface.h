/*
 * include/framework/QueryInterface.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <concepts>
#include "util/types.h"

template <typename Q>
concept QueryInterface = requires(Q q, void *p) {
    {q.get_query_state(p, p)} -> std::convertible_to<void*>;
    {q.get_buffer_query_state(p, p)};
    {q.query(p, p)};
    {q.buffer_query(p, p)};
    {q.merge()};
    {q.delete_query_state(p)};
};
