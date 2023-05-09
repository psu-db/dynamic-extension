/*
 *
 *
 */

#pragma once

#include <string>

#include <unistd.h>
#include <fcntl.h>

#include "util/types.h"
#include "util/base.h"
#include "framework/MutableBuffer.h"
#include "framework/InternalLevel.h"

typedef de::Record<uint64_t, uint32_t, uint64_t> WeightedRec;
typedef de::MutableBuffer<uint64_t, uint32_t, uint64_t> WeightedMBuffer;
typedef de::InternalLevel<uint64_t, uint32_t, uint64_t> WeightedLevel;

typedef de::Record<uint64_t, uint32_t> UnweightedRec;
typedef de::MutableBuffer<uint64_t, uint32_t> UnweightedMBuffer;
typedef de::InternalLevel<uint64_t, uint32_t> UnweightedLevel;

static gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

static bool initialize_test_file(std::string fname, size_t page_cnt)
{
    auto flags = O_RDWR | O_CREAT | O_TRUNC;
    mode_t mode = 0640;
    char *page = nullptr;

    int fd = open(fname.c_str(), flags, mode);
    if (fd == -1) {
        goto error;
    }

    page = (char *) aligned_alloc(de::SECTOR_SIZE, de::PAGE_SIZE);
    if (!page) {
        goto error_opened;
    }

    for (size_t i=0; i<=page_cnt; i++) {
        *((int *) page) = i;
        if (write(fd, page, de::PAGE_SIZE) == -1) {
            goto error_alloced;
        }
    }

    free(page);

    return 1;

error_alloced:
    free(page);

error_opened:
    close(fd);

error:
    return 0;
}

static bool roughly_equal(int n1, int n2, size_t mag, double epsilon) {
    return ((double) std::abs(n1 - n2) / (double) mag) < epsilon;
}

static WeightedMBuffer *create_test_mbuffer(size_t cnt)
{
    auto buffer = new WeightedMBuffer(cnt, true, cnt, g_rng);

    for (size_t i = 0; i < cnt; i++) {
        uint64_t key = rand();
        uint32_t val = rand();

        buffer->append(key, val);
    }

    return buffer;
}

static WeightedMBuffer *create_test_mbuffer_tombstones(size_t cnt, size_t ts_cnt) 
{
    auto buffer = new WeightedMBuffer(cnt, true, ts_cnt, g_rng);

    std::vector<std::pair<uint64_t, uint32_t>> tombstones;

    for (size_t i = 0; i < cnt; i++) {
        uint64_t key = rand();
        uint32_t val = rand();

        if (i < ts_cnt) {
            tombstones.push_back({key, val});
        }

        buffer->append(key, val);
    }

    for (size_t i=0; i<ts_cnt; i++) {
        buffer->append(tombstones[i].first, tombstones[i].second, 1.0, true);
    }

    return buffer;
}

static WeightedMBuffer *create_weighted_mbuffer(size_t cnt)
{
    auto buffer = new WeightedMBuffer(cnt, true, cnt, g_rng);
    
    // Put in half of the count with weight one.
    uint64_t key = 1;
    for (size_t i=0; i< cnt / 2; i++) {
        buffer->append(key, i, 2);
    }

    // put in a quarter of the count with weight two.
    key = 2;
    for (size_t i=0; i< cnt / 4; i++) {
        buffer->append(key, i, 4);
    }

    // the remaining quarter with weight four.
    key = 3;
    for (size_t i=0; i< cnt / 4; i++) {
        buffer->append(key, i, 8);
    }

    return buffer;
}

static WeightedMBuffer *create_double_seq_mbuffer(size_t cnt, bool ts=false) 
{
    auto buffer = new WeightedMBuffer(cnt, true, cnt, g_rng);

    for (size_t i = 0; i < cnt / 2; i++) {
        uint64_t key = i;
        uint32_t val = i;

        buffer->append(key, val, 1.0, ts);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        uint64_t key = i;
        uint32_t val = i + 1;

        buffer->append(key, val, 1.0, ts);
    }

    return buffer;
}


