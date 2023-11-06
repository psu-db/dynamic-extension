/*
 * include/framework/scheduling/SerialScheduler.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 * IMPORTANT: This "scheduler" is a shim implementation for allowing
 * strictly serial, single-threaded operation of the framework. It should 
 * never be used in multi-threaded contexts. A call to the schedule_job 
 * function will immediately run the job and block on its completion before
 * returning.
 *
 */
#pragma once

#include <vector>
#include <memory>
#include <queue>
#include <thread>
#include <condition_variable>
#include <future>

#include "util/types.h"
#include "framework/interface/Shard.h"
#include "framework/interface/Query.h"
#include "framework/interface/Record.h"
#include "framework/structure/MutableBuffer.h"
#include "framework/util/Configuration.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/scheduling/Task.h"

namespace de {

class SerialScheduler {
public:
    SerialScheduler(size_t memory_budget, size_t thread_cnt) 
      : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX)
      , m_thrd_cnt((thread_cnt) ? thread_cnt: UINT64_MAX)
      , m_used_memory(0)
      , m_used_thrds(0)
      , m_counter(0)
    {}

    ~SerialScheduler() = default;

    void schedule_job(std::function<void(void*)> job, size_t size, void *args) {
        size_t ts = m_counter++;
        auto t = Task(size, ts, job, args);
        t(0);
    }

    void shutdown() {
        /* intentionally left blank */
    }

private:
    size_t m_memory_budget;
    size_t m_thrd_cnt;

    size_t m_used_thrds;
    size_t m_used_memory;

    size_t m_counter;
};

}
