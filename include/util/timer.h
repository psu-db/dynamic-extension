/*
 * include/util/timer.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <chrono>

#ifdef ENABLE_TIMER
#define TIMER_INIT() \
    auto timer_start = std::chrono::high_resolution_clock::now(); \
    auto timer_stop = std::chrono::high_resolution_clock::now();

#define TIMER_START() \
    timer_start = std::chrono::high_resolution_clock::now()

#define TIMER_STOP() \
    timer_stop = std::chrono::high_resolution_clock::now()

#define TIMER_RESULT() \
    std::chrono::duration_cast<std::chrono::nanoseconds>(timer_stop - timer_start).count()

#else
    #define TIMER_INIT() \
        do {} while(0)
    #define TIMER_START() \
        do {} while(0)
    #define TIMER_STOP() \
        do {} while(0)
    #define TIMER_RESULT() \
        0l
#endif

