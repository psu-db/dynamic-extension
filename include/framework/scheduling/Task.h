/*
 *
 */
#pragma once

#include <variant>

#include "framework/util/Configuration.h"

namespace de {

enum class TaskType {
    MERGE,
    QUERY
};

struct MergeTask {
    level_index m_source_level;
    level_index m_target_level;
    size_t m_timestamp;
    size_t m_size;
    TaskType m_type;

    TaskType get_type() const {
        return m_type;
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

    TaskType get_type() const {
        return m_type;
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
