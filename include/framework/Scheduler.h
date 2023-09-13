/*
 * include/framework/Scheduler.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <memory>
#include <queue>

#include "util/types.h"
#include "framework/ShardInterface.h"
#include "framework/QueryInterface.h"
#include "framework/RecordInterface.h"
#include "framework/MutableBuffer.h"
#include "framework/Configuration.h"
#include "framework/ExtensionStructure.h"

namespace de {


struct MergeTask {
    level_index m_source_level;
    level_index m_target_level;
    size_t m_size;
    size_t m_timestamp;

    bool operator<(MergeTask &other) {
        return m_timestamp < other.m_timestamp;
    }
};


template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L>
class Scheduler {
    typedef ExtensionStructure<R, S, Q, L> Structure;
public:
    /*
     * Memory budget stated in bytes, with 0 meaning unlimited. Likewise, 0 threads means 
     * unlimited.
     */
    Scheduler(size_t memory_budget, size_t thread_cnt) 
        : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX)
        , m_thread_cnt((thread_cnt) ? thread_cnt : UINT64_MAX)
        , m_used_memory(0)
        , m_used_threads(0)
    {}

    bool schedule_merge(Structure *version, MutableBuffer<R> *buffer) {
        // FIXME: this is a non-concurrent implementation
        return version->merge_buffer(buffer);
    }

private:
    size_t get_timestamp() {
        auto ts = m_timestamp.fetch_add(1);
        return ts;
    }

    size_t m_memory_budget;
    size_t m_thread_cnt;

    alignas(64) std::atomic<size_t> m_used_memory;
    alignas(64) std::atomic<size_t> m_used_threads;
    alignas(64) std::atomic<size_t> m_timestamp;

    std::priority_queue<MergeTask> m_merge_queue;
    std::mutex m_merge_queue_lock;
};

}
