/*
 * include/framework/scheduling/statistics.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <cassert>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <chrono>
#include <atomic>

namespace de {

class SchedulerStatistics {
private:
    enum class EventType {
        QUEUED,
        SCHEDULED,
        STARTED, 
        FINISHED
    };

    struct Event {
        size_t id;
        EventType type;
    };

    struct JobInfo {
        size_t id;
        size_t size;
        size_t type;
    };


public:
    SchedulerStatistics() = default;
    ~SchedulerStatistics() = default;

    void job_queued(size_t id, size_t type, size_t size) {
        auto time = std::chrono::high_resolution_clock::now();
    }

    void job_scheduled(size_t id) {
        std::unique_lock<std::mutex> lk(m_mutex);

    }

    void job_begin(size_t id) {

    }

    void job_complete(size_t id) {

    }

    /* FIXME: This is just a temporary approach */
    void log_time_data(size_t length, size_t type) {
        assert(type == 1 || type == 2);

        if (type == 1) {
            m_type_1_cnt.fetch_add(1);
            m_type_1_total_time.fetch_add(length);
            
            if (length > m_type_1_largest_time) {
                m_type_1_largest_time.store(length);
            }
        } else {
            m_type_2_cnt.fetch_add(1);
            m_type_2_total_time.fetch_add(length);

            if (length > m_type_2_largest_time) {
                m_type_2_largest_time.store(length);
            }
        }
    }

    void print_statistics() {
        if (m_type_1_cnt > 0) {
            fprintf(stdout, "Query Count: %ld\tQuery Avg. Latency: %ld\tMax Query Latency: %ld\n", 
                    m_type_1_cnt.load(), 
                    m_type_1_total_time.load() / m_type_1_cnt.load(),
                    m_type_1_largest_time.load());
        }
        if (m_type_2_cnt > 0) {
            fprintf(stdout, "Reconstruction Count: %ld\tReconstruction Avg. Latency: %ld\tMax Recon. Latency:%ld\n",
                    m_type_2_cnt.load(),
                    m_type_2_total_time.load() / m_type_2_cnt.load(),
                    m_type_2_largest_time.load());
        }
    }

private:
    std::mutex m_mutex;
    std::unordered_map<size_t, JobInfo> m_jobs;
    std::vector<Event> m_event_log;

    std::atomic<size_t> m_type_1_cnt;
    std::atomic<size_t> m_type_1_total_time;

    std::atomic<size_t> m_type_2_cnt;
    std::atomic<size_t> m_type_2_total_time;

    std::atomic<size_t> m_type_1_largest_time;
    std::atomic<size_t> m_type_2_largest_time;
};
}
