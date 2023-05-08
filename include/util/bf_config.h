/*
 * include/util/bf_config.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include "util/base.h"

namespace de {

static double BF_FPR = .01;
static size_t BF_HASH_FUNCS = 7;

static void BF_SET_FPR(double fpr) {
    BF_FPR = fpr;
}

static void BF_SET_HASHFUNC(size_t func_cnt) {
    BF_HASH_FUNCS = func_cnt;
}

}
