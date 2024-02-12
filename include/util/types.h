/*
 * include/util/types.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A centralized header file for various data types used throughout the
 * code base. There are a few very specific types, such as header formats,
 * that are defined within the header files that make direct use of them,
 * but all generally usable, simple types are defined here.
 *
 * Many of these types were used in the Practical Dynamic Extension for
 * Sampling Indexes work, particularly for external storage and buffer
 * pool systems. They aren't used now, but we're leaving them here to use
 * them in the future, when we add this functionality into this system too.
 */
#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>
#include <cassert>

namespace de {

/* Represents a page offset within a specific file (physical or virtual) */
typedef uint32_t PageNum;

/*
 * Byte offset within a page. Also used for lengths of records, etc.,
 * within the codebase. size_t isn't necessary, as the maximum offset
 * is only parm::PAGE_SIZE 
 */
typedef uint16_t PageOffset;

/* A unique identifier for a frame within a buffer or cache */
typedef int32_t FrameId;

/* 
 * A unique timestamp for use in MVCC concurrency control. Currently stored in
 * record headers, but not used by anything.
 */
typedef uint32_t Timestamp;
const Timestamp TIMESTAMP_MIN = 0;
const Timestamp TIMESTAMP_MAX = UINT32_MAX;

/* 
 * Invalid values for various IDs. Used throughout the code base to indicate
 * uninitialized values and error conditions.
 */
const PageNum INVALID_PNUM = 0;
const FrameId INVALID_FRID = -1;

/*
 * An ID for a given shard within the index. The level_idx is the index
 * in the memory_levels and disk_levels vectors corresponding to the
 * shard, and the shard_idx is the index with the level (always 0 in the
 * case of leveling). Note that the two vectors of levels are treated
 * as a contiguous index space.
 */
struct ShardID {
    ssize_t level_idx;
    ssize_t shard_idx;

    friend bool operator==(const ShardID &shid1, const ShardID &shid2) {
        return shid1.level_idx == shid2.level_idx && shid1.shard_idx == shid2.shard_idx;
    }
};

/* A placeholder for an invalid shard--also used to indicate the mutable buffer */
const ShardID INVALID_SHID = {-1, -1};

typedef ssize_t level_index;

typedef struct {
    level_index source;
    level_index target;
    size_t reccnt;
} ReconstructionTask;

class ReconstructionVector {
public:
    ReconstructionVector() 
    : total_reccnt(0) {}

    ~ReconstructionVector() = default;

    ReconstructionTask operator[](size_t idx) {
        return m_tasks[idx];
    }

    void add_reconstruction(level_index source, level_index target, size_t reccnt) {
        m_tasks.push_back({source, target, reccnt});
        total_reccnt += reccnt;
    }

    ReconstructionTask remove_reconstruction(size_t idx) {
        assert(idx < m_tasks.size());
        auto task = m_tasks[idx];

        m_tasks.erase(m_tasks.begin() + idx);
        total_reccnt -= task.reccnt;

        return task;
    }

    ReconstructionTask remove_smallest_reconstruction() {
        size_t min_size = m_tasks[0].reccnt;
        size_t idx = 0;
        for (size_t i=1; i<m_tasks.size(); i++) {
            if (m_tasks[i].reccnt < min_size) {
                min_size = m_tasks[i].reccnt;
                idx = i;
            }
        }

        auto task = m_tasks[idx];
        m_tasks.erase(m_tasks.begin() + idx);
        total_reccnt -= task.reccnt;

        return task;
    }

    size_t get_total_reccnt() {
        return total_reccnt;
    }

    size_t size() {
        return m_tasks.size();
    }


private:
    std::vector<ReconstructionTask> m_tasks;
    size_t total_reccnt;
};

}
