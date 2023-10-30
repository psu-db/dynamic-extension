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

#include "psu-ds/LockedPriorityQueue.h"

namespace de {

class FIFOScheduler {
public:
    FIFOScheduler(size_t memory_budget, size_t thread_cnt)
      : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX)
      , m_thrd_cnt((thread_cnt) ? thread_cnt: UINT64_MAX)
      , m_used_memory(0)
      , m_used_thrds(0)
      , m_shutdown(false)
    {
        m_sched_thrd = std::thread(&FIFOScheduler::run, this);
    }

    ~FIFOScheduler() {
        shutdown();

        std::unique_lock<std::mutex> lk(m_cv_lock);
        m_cv.notify_all();
        lk.release();

        m_sched_thrd.join();
    }

    void schedule_job(std::function<void(void*)> job, size_t size, void *args) {
        size_t ts = m_counter.fetch_add(1);
        m_task_queue.push(Task(size, ts, job, args));

        std::unique_lock<std::mutex> lk(m_cv_lock);
        m_cv.notify_all();
    }

    void shutdown() {
        m_shutdown = true;
    }

private:
    psudb::LockedPriorityQueue<Task> m_task_queue;

    size_t m_memory_budget;
    size_t m_thrd_cnt;

    bool m_shutdown; 

    std::atomic<size_t> m_counter;
    std::mutex m_cv_lock;
    std::condition_variable m_cv;

    std::thread m_sched_thrd;

    std::atomic<size_t> m_used_thrds;
    std::atomic<size_t> m_used_memory;

    void schedule_next() {
        auto t = m_task_queue.pop();
        t();
    }

    void run() {
        do {
            std::unique_lock<std::mutex> cv_lock(m_cv_lock);
            m_cv.wait(cv_lock);

            while (m_task_queue.size() > 0 && m_used_thrds.load() < m_thrd_cnt) {
                schedule_next();
            }
            cv_lock.unlock();
        } while(!m_shutdown);
    }
};

}
