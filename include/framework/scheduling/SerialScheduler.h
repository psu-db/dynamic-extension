/*
 * include/framework/scheduling/SerialScheduler.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * IMPORTANT: This "scheduler" is a shim implementation for allowing
 * strictly serial, single-threaded operation of the framework. It should
 * never be used in multi-threaded contexts. A call to the schedule_job
 * function will immediately run the job and block on its completion before
 * returning.
 *
 */
#pragma once

#include "framework/scheduling/Task.h"
#include "framework/scheduling/statistics.h"

namespace de {

class SerialScheduler {
public:
  SerialScheduler(size_t memory_budget, size_t thread_cnt)
      : m_memory_budget((memory_budget) ? memory_budget : UINT64_MAX),
        m_thrd_cnt((thread_cnt) ? thread_cnt : UINT64_MAX), m_used_memory(0),
        m_used_thrds(0), m_counter(0) {}

  ~SerialScheduler() = default;

  void schedule_job(std::function<void(void *)> job, size_t size, void *args,
                    size_t type = 0) {
    size_t ts = m_counter++;
    m_stats.job_queued(ts, type, size);
    m_stats.job_scheduled(ts);
    auto t = Task(size, ts, job, args, type, &m_stats);
    t(0);
  }

  void shutdown() { /* intentionally left blank */ }

  void print_statistics() { m_stats.print_statistics(); }

private:
  [[maybe_unused]] size_t m_memory_budget;
  [[maybe_unused]] size_t m_thrd_cnt;

  [[maybe_unused]] size_t m_used_memory;
  [[maybe_unused]] size_t m_used_thrds;

  size_t m_counter;

  SchedulerStatistics m_stats;
};

} // namespace de
