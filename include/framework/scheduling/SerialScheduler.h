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
#include <thread>
#include <condition_variable>

#include "util/types.h"
#include "framework/interface/Shard.h"
#include "framework/interface/Query.h"
#include "framework/interface/Record.h"
#include "framework/structure/MutableBuffer.h"
#include "framework/util/Configuration.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/scheduling/Task.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L>
class SerialScheduler {
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef MutableBuffer<R> Buffer;
public:
    /*
     * A simple "scheduler" that runs tasks serially, in a FIFO manner. Incoming concurrent
     * requests will wait for their turn, and only one task will be active in the system at
     * a time. The scheduler will spin up a second thread for running itself, but all tasks
     * will be single-threaded. 
     *
     * Memory budget stated in bytes, with 0 meaning unlimited. Likewise, 0 threads means 
     * unlimited.
     *
     * Note that the SerialScheduler object is non-concurrent, and so will ignore the
     * thread_cnt argument. It will obey the memory_budget, however a failure due to
     * memory constraints will be irrecoverable, as there is no way to free up memory
     * or block particular tasks until memory becomes available.
     */
    SerialScheduler(size_t memory_budget, size_t thread_cnt) 
        : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX)
        , m_thread_cnt((thread_cnt) ? thread_cnt : UINT64_MAX)
        , m_used_memory(0)
        , m_used_threads(0)
        , m_shutdown(false)
    { 
        m_sched_thrd = std::thread(&SerialScheduler::run_scheduler, this);
    }

    ~SerialScheduler() {
        m_shutdown = true;

        m_cv.notify_all();
        m_sched_thrd.join();
    }

    bool schedule_merge(Structure *version, MutableBuffer<R> *buffer) {
        pending_version = version;
        pending_buffer = buffer;

        /*
         * Get list of individual level reconstructions that are necessary
         * for completing the overall merge
         */
        std::vector<MergeTask> merges = version->get_merge_tasks(buffer->get_record_count());

        /*
         * Schedule the merge tasks (FIXME: currently this just 
         * executes them sequentially in a blocking fashion)
         */
        for (ssize_t i=0; i<merges.size(); i++) {
            merges[i].m_timestamp = m_timestamp.fetch_add(1);
            m_merge_queue_lock.lock();
            m_merge_queue.emplace(merges[i]);
            m_merge_queue_lock.unlock();
        }

        MergeTask buffer_merge;
        buffer_merge.m_source_level = -1;
        buffer_merge.m_target_level = 0;
        buffer_merge.m_size = buffer->get_record_count() * sizeof(R) * 2;
        buffer_merge.m_timestamp = m_timestamp.fetch_add(1);
        buffer_merge.m_type = TaskType::MERGE;
        m_merge_queue_lock.lock();
        m_merge_queue.emplace(buffer_merge);
        m_merge_queue_lock.unlock();

        m_cv.notify_all();
        do {
            std::unique_lock<std::mutex> merge_cv_lock(m_merge_cv_lock);
            m_merge_cv.wait(merge_cv_lock);
        } while (m_merge_queue.size() > 0);

        assert(version->get_levels()[version->get_levels().size() - 1]->get_shard(0)->get_tombstone_count() == 0);

        return true;
    }

    bool schedule_query() {
        return true;
    }

private:
    size_t get_timestamp() {
        auto ts = m_timestamp.fetch_add(1);
        return ts;
    }

    void schedule_merge(MergeTask task) {
        if (task.m_source_level == -1 && task.m_target_level == 0) {
            run_buffer_merge(pending_buffer, pending_version);
        } else {
            run_merge(task, pending_version);
        }
    }


    void schedule_query(QueryTask task) {

    }

    void schedule_next_task() {
        m_merge_queue_lock.lock();
        auto task = m_merge_queue.top();
        m_merge_queue.pop();
        m_merge_queue_lock.unlock();

        auto type = std::visit(GetTaskType{}, task);

        switch (type) {
            case TaskType::MERGE: 
                schedule_merge(std::get<MergeTask>(task));
                break;
            case TaskType::QUERY: 
                schedule_query(std::get<QueryTask>(task));
                break;
            default: assert(false);
        }

        if (m_merge_queue.size() == 0) {
            m_merge_cv.notify_all();
        }
    }


    void run_merge(MergeTask task, Structure *version) {
        version->merge_levels(task.m_target_level, task.m_source_level); 

        if (!version->validate_tombstone_proportion(task.m_target_level)) {
            auto tasks = version->get_merge_tasks(task.m_target_level);    
            /*
             * Schedule the merge tasks (FIXME: currently this just 
             * executes them sequentially in a blocking fashion)
             */
            for (ssize_t i=tasks.size()-1; i>=0; i--) {
                tasks[i].m_timestamp = m_timestamp.fetch_add(1);
                m_merge_queue_lock.lock();
                m_merge_queue.push(tasks[i]);
                m_merge_queue_lock.unlock();
            }
        }
    }


    void run_buffer_merge(Buffer *buffer, Structure *version) {
        version->merge_buffer(buffer);
        if (!version->validate_tombstone_proportion(0)) {
            auto tasks = version->get_merge_tasks_from_level(0);

            /*
             * Schedule the merge tasks (FIXME: currently this just 
             * executes them sequentially in a blocking fashion)
             */
            for (ssize_t i=tasks.size()-1; i>=0; i--) {
                tasks[i].m_timestamp = m_timestamp.fetch_add(1);
                m_merge_queue_lock.lock();
                m_merge_queue.push(tasks[i]);
                m_merge_queue_lock.unlock();
            }
        }
    }

    void run_scheduler() {
        do {
            std::unique_lock<std::mutex> cv_lock(m_cv_lock);
            m_cv.wait(cv_lock);

            while (m_merge_queue.size() > 0 && m_used_threads.load() < m_thread_cnt) {
                schedule_next_task();
            }
            cv_lock.unlock();
        } while(!m_shutdown);
    }

    size_t m_memory_budget;
    size_t m_thread_cnt;

    Buffer *pending_buffer;
    Structure *pending_version;

    alignas(64) std::atomic<size_t> m_used_memory;
    alignas(64) std::atomic<size_t> m_used_threads;
    alignas(64) std::atomic<size_t> m_timestamp;

    std::priority_queue<Task, std::vector<Task>, std::greater<Task>> m_merge_queue;
    std::mutex m_merge_queue_lock;

    std::mutex m_cv_lock;
    std::condition_variable m_cv;

    std::mutex m_merge_cv_lock;
    std::condition_variable m_merge_cv;

    std::thread m_sched_thrd;

    bool m_shutdown;
};

}
