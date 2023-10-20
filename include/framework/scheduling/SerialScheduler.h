/*
 * include/framework/Scheduler.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
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
#include <future>

#include "util/types.h"
#include "framework/interface/Shard.h"
#include "framework/interface/Query.h"
#include "framework/interface/Record.h"
#include "framework/structure/MutableBuffer.h"
#include "framework/util/Configuration.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/scheduling/Task.h"

#include "psu-ds/LockedPriorityQueue.h"

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
         * Schedule the merge tasks 
         */
        for (ssize_t i=0; i<merges.size(); i++) {
            merges[i].m_timestamp = m_timestamp.fetch_add(1);
            m_task_queue.push(merges[i]);
        }

        auto t = MergeTask(-1, 0, buffer->get_record_count() * sizeof(R) * 2, m_timestamp.fetch_add(1));
        m_task_queue.push(t);

        m_cv.notify_all();
        do {
            std::unique_lock<std::mutex> merge_cv_lock(m_merge_cv_lock);
            m_merge_cv.wait(merge_cv_lock);
        } while (m_task_queue.size() > 0);

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
        auto task = m_task_queue.pop();

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

        if (m_task_queue.size() == 0) {
            m_merge_cv.notify_all();
        }
    }


    void run_merge(MergeTask task, Structure *version) {
        version->merge_levels(task.m_target_level, task.m_source_level); 

        if (!version->validate_tombstone_proportion(task.m_target_level)) {
            auto tasks = version->get_merge_tasks(task.m_target_level);    
            /*
             * Schedule the merge tasks 
             */
            std::promise<void> trigger_prom;
            tasks[tasks.size() - 1].make_dependent_on(trigger_prom);
            tasks[tasks.size() - 1].m_timestamp = m_timestamp.fetch_add(1);
            m_task_queue.push(tasks[tasks.size() - 1]);

            for (ssize_t i=tasks.size()-2; i>=0; i--) {
                tasks[i].make_dependent_on(tasks[i+1]);
                tasks[i].m_timestamp = m_timestamp.fetch_add(1);
                m_task_queue.push(tasks[i]);
            }

            /*
             * Block the completion of any task until all have been
             * scheduled. Probably not strictly necessary, but due to
             * interface constraints with the way promises are used, 
             * a dummy promise needs to be set up for the first job 
             * anyway. It's easiest to just release it here.
             */
            trigger_prom.set_value();
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
                m_task_queue.push(tasks[i]);
            }
        }
    }

    void run_scheduler() {
        do {
            std::unique_lock<std::mutex> cv_lock(m_cv_lock);
            m_cv.wait(cv_lock);

            while (m_task_queue.size() > 0 && m_used_threads.load() < m_thread_cnt) {
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

    psudb::LockedPriorityQueue<Task, std::vector<Task>, std::greater<Task>> m_task_queue;

    std::mutex m_cv_lock;
    std::condition_variable m_cv;

    std::mutex m_merge_cv_lock;
    std::condition_variable m_merge_cv;

    std::thread m_sched_thrd;

    bool m_shutdown;
};

}
