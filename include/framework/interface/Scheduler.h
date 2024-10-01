/*
 * include/framework/interface/Scheduler.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/scheduling/Task.h"

template <typename SchedType>
concept SchedulerInterface = requires(SchedType s, size_t i, void *vp, de::Job j) {
    {SchedType(i, i)};
    {s.schedule_job(j, i, vp, i)} -> std::convertible_to<void>;
    {s.shutdown()};
    {s.print_statistics()};
};
