/*
 * include/framework/util/Configuration.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <utility>

namespace de {

static thread_local size_t sampling_attempts = 0;
static thread_local size_t sampling_rejections = 0;
static thread_local size_t deletion_rejections = 0;
static thread_local size_t bounds_rejections = 0;
static thread_local size_t tombstone_rejections = 0;
static thread_local size_t buffer_rejections = 0;

/*
 * thread_local size_t various_sampling_times go here.
 */
static thread_local size_t sample_range_time = 0;
static thread_local size_t alias_time = 0;
static thread_local size_t alias_query_time = 0;
static thread_local size_t rejection_check_time = 0;
static thread_local size_t buffer_sample_time = 0;
static thread_local size_t memlevel_sample_time = 0;
static thread_local size_t disklevel_sample_time = 0;
static thread_local size_t sampling_bailouts = 0;


enum class LayoutPolicy {
    LEVELING,
    TEIRING
};

enum class DeletePolicy {
    TOMBSTONE,
    TAGGING
};

}
