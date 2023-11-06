/*
 * include/framework/scheduling/FIFOScheduler.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
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

#include "ctpl/ctpl.h"
#include "psu-ds/LockedPriorityQueue.h"

namespace de {


class FIFOScheduler {
private:
    static const size_t DEFAULT_MAX_THREADS = 8;

public:
    FIFOScheduler(size_t memory_budget, size_t thread_cnt)
      : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX)
      , m_thrd_cnt((thread_cnt) ? thread_cnt: DEFAULT_MAX_THREADS)
      , m_used_memory(0)
      , m_used_thrds(0)
      , m_shutdown(false)
    {
        m_sched_thrd = std::thread(&FIFOScheduler::run, this);
        m_thrd_pool.resize(m_thrd_cnt);
    }

    ~FIFOScheduler() {
        shutdown();

        m_cv.notify_all();
        m_sched_thrd.join();
    }

    void schedule_job(std::function<void(void*)> job, size_t size, void *args) {
        std::unique_lock<std::mutex> lk(m_cv_lock);
        size_t ts = m_counter.fetch_add(1);
        m_task_queue.push(Task(size, ts, job, args));

        m_cv.notify_all();
    }

    void shutdown() {
        m_shutdown.store(true);
    }

private:
    psudb::LockedPriorityQueue<Task> m_task_queue;

    size_t m_memory_budget;
    size_t m_thrd_cnt;

    std::atomic<bool> m_shutdown; 

    std::atomic<size_t> m_counter;
    std::mutex m_cv_lock;
    std::condition_variable m_cv;

    std::thread m_sched_thrd;
    ctpl::thread_pool m_thrd_pool;

    std::atomic<size_t> m_used_thrds;
    std::atomic<size_t> m_used_memory;

    void schedule_next() {
        assert(m_task_queue.size() > 0);
        auto t = m_task_queue.pop();
        m_thrd_pool.push(t);
    }

    void run() {
        do {
            std::unique_lock<std::mutex> cv_lock(m_cv_lock);
            m_cv.wait(cv_lock);

            while (m_task_queue.size() > 0 && m_thrd_pool.n_idle() > 0) {
                schedule_next();
            }
        } while(!m_shutdown.load());
    }
};

}
