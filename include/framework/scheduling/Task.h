/*
 *
 */
#pragma once

#include <variant>
#include <future>

#include "framework/util/Configuration.h"

namespace de {

enum class TaskType {
    MERGE,
    QUERY
};

struct TaskDependency {
    std::promise<void> prom;
    std::future<void> fut;
};

struct MergeTask {
    level_index m_source_level;
    level_index m_target_level;
    size_t m_timestamp;
    size_t m_size;
    TaskType m_type;
    std::unique_ptr<TaskDependency> m_dep;

    MergeTask() = default;

    MergeTask(level_index source, level_index target, size_t size, size_t timestamp)
    : m_source_level(source)
    , m_target_level(target)
    , m_timestamp(timestamp)
    , m_size(size)
    , m_type(TaskType::MERGE)
    , m_dep(std::make_unique<TaskDependency>()){}


    MergeTask(MergeTask &t)
    : m_source_level(t.m_source_level)
    , m_target_level(t.m_target_level)
    , m_timestamp(t.m_timestamp)
    , m_size(t.m_size)
    , m_type(TaskType::MERGE)
    , m_dep(std::move(t.m_dep))
    {}


    TaskType get_type() const {
        return m_type;
    }

    void make_dependent_on(MergeTask &task) {
        m_dep->fut = task.m_dep->prom.get_future();
    }

    void make_dependent_on(TaskDependency *dep) {
        m_dep->fut = dep->prom.get_future();
    }

    friend bool operator<(const MergeTask &self, const MergeTask &other) {
        return self.m_timestamp < other.m_timestamp;
    }

    friend bool operator>(const MergeTask &self, const MergeTask &other) {
        return self.m_timestamp > other.m_timestamp;
    }

};

struct QueryTask {
    size_t m_timestamp;
    size_t m_size;
    TaskType m_type;
    std::unique_ptr<TaskDependency> m_dep;

    QueryTask(QueryTask &t) 
        : m_timestamp(t.m_timestamp)
        , m_size(t.m_size)
        , m_type(t.m_type)
        , m_dep(std::move(t.m_dep))
    {}

    TaskType get_type() const {
        return m_type;
    }

    void SetDependency(QueryTask &task) {
        m_dep->fut = task.m_dep->prom.get_future();
    }

    void SetDependency(TaskDependency *dep) {
        m_dep->fut = dep->prom.get_future();
    }

    friend bool operator<(const QueryTask &self, const QueryTask &other) {
        return self.m_timestamp < other.m_timestamp;
    }

    friend bool operator>(const QueryTask &self, const QueryTask &other) {
        return self.m_timestamp > other.m_timestamp;
    }
};

struct GetTaskType {
    TaskType operator()(const MergeTask &t) { return t.get_type(); }
    TaskType operator()(const QueryTask &t) { return t.get_type(); }
};

typedef std::variant<MergeTask, QueryTask> Task;

}
