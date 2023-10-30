/*
 * include/framework/interface/Scheduler.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <concepts>
#include "framework/interface/Record.h"
#include "util/types.h"
#include "framework/scheduling/Task.h"

template <typename S>
concept SchedulerInterface = requires(S s, size_t i, void *vp, de::Job j) {
    {S(i, i)};
    {s.schedule_job(j, i, vp)} -> std::convertible_to<void>;
    {s.shutdown()};
};
