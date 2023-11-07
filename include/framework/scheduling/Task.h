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

#include "framework/util/Configuration.h"
#include "framework/scheduling/Epoch.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L>
struct MergeArgs {
    Epoch<R, S, Q, L> *epoch;
    std::vector<MergeTask> merges;
    std::promise<bool> result;
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
    Task(size_t size, size_t ts, Job job, void *args) 
      : m_job(job)
      , m_size(size)
      , m_timestamp(ts)
      , m_args(args)
    {}

    Job m_job;
    size_t m_size;
    size_t m_timestamp;
    void *m_args;

    friend bool operator<(const Task &self, const Task &other) {
        return self.m_timestamp < other.m_timestamp;
    }

    friend bool operator>(const Task &self, const Task &other) {
        return self.m_timestamp > other.m_timestamp;
    }

    void operator()(size_t thrd_id) {
        m_job(m_args);
    }
};

}
