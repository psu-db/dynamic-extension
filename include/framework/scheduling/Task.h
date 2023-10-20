/*
 *
 */
#pragma once

#include <variant>
#include <future>
#include <functional>

#include "framework/util/Configuration.h"

namespace de {

struct MergeArgs {
    void *version;
    void *buffer;
    std::vector<MergeTask> merges;
    std::promise<bool> result;
};

template <typename R>
struct QueryArgs {
    void *version;
    void *buffer;
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

    void operator()() {
        m_job(m_args);
    }
};

}
