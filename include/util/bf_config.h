/*
 * include/util/bf_config.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * Global parameters for configuring bloom filters used as auxiliary
 * structures on shards within the framework. The bloom filter class
 * can be found in
 *
 * $PROJECT_ROOT/external/psudb-common/cpp/include/psu-ds/BloomFilter.h
 *
 */
#pragma once

#include "psu-util/alignment.h"

namespace de {

/* global variable for specifying bloom filter FPR */
static double BF_FPR = .01;

/* global variable for specifying number of BF hash functions (k) */
static size_t BF_HASH_FUNCS = 7;

/*
 * Adjust the value of BF_FPR. The argument must be on the interval 
 * (0, 1), or the behavior of bloom filters is undefined.
 */
static void BF_SET_FPR(double fpr) {

    BF_FPR = fpr;
}

/*
 * Adjust the value of BF_HASH_FUNCS. The argument must be on the interval
 * (0, INT64_MAX], or the behavior of bloom filters is undefined.
 */
static void BF_SET_HASHFUNC(size_t func_cnt) {
    BF_HASH_FUNCS = func_cnt;
}

}
