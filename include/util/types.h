/*
 * include/util/types.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 * A centralized header file for various data types used throughout the
 * code base. There are a few very specific types, such as header formats,
 * that are defined within the header files that make direct use of them,
 * but all generally usable, simple types are defined here.
 *
 */
#pragma once

#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include <string>

namespace de {

using std::byte;

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

}
