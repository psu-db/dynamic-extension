/*
 * include/framework/scheduling/Task.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <future>
#include <functional>
#include <chrono>

#include "framework/util/Configuration.h"
#include "framework/scheduling/Epoch.h"
#include "framework/scheduling/statistics.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L>
struct ReconstructionArgs {
    Epoch<R, S, Q, L> *epoch;
    std::vector<ReconstructionTask> merges;
    std::promise<bool> result;
    bool compaction;
    void *extension;
};

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L>
struct QueryArgs {
    Epoch<R, S, Q, L> *epoch;
    std::promise<std::vector<R>> result_set;
    void *query_parms;
};

typedef std::function<void(void*)> Job;

struct Task {
    Task(size_t size, size_t ts, Job job, void *args, size_t type=0, SchedulerStatistics *stats=nullptr) 
      : m_job(job)
      , m_size(size)
      , m_timestamp(ts)
      , m_args(args)
      , m_type(type)
      , m_stats(stats)
    {}

    Job m_job;
    size_t m_size;
    size_t m_timestamp;
    void *m_args;
    size_t m_type;
    SchedulerStatistics *m_stats;

    friend bool operator<(const Task &self, const Task &other) {
        return self.m_timestamp < other.m_timestamp;
    }

    friend bool operator>(const Task &self, const Task &other) {
        return self.m_timestamp > other.m_timestamp;
    }

    void operator()(size_t thrd_id) {
        auto start = std::chrono::high_resolution_clock::now();
        if (m_stats) {
            m_stats->job_begin(m_timestamp);
        }

        m_job(m_args);

        if (m_stats) {
            m_stats->job_complete(m_timestamp);
        }
        auto stop = std::chrono::high_resolution_clock::now();

        if (m_stats) {
            auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
            m_stats->log_time_data(time, m_type);
        }
    }
};

}
