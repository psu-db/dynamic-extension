/*
 * tests/testing.h
 *
 * Unit test utility functions/definitions
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <string>

#include <unistd.h>
#include <fcntl.h>

#include "util/types.h"
#include "util/base.h"
#include "framework/MutableBuffer.h"

typedef de::WeightedRecord<uint64_t, uint32_t, uint64_t> WRec;
typedef de::Record<uint64_t, uint32_t> Rec;
typedef de::WrappedRecord<WRec> WrappedWRec;
typedef de::WrappedRecord<Rec> WrappedRec;

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

template <de::RecordInterface R>
static de::MutableBuffer<R> *create_test_mbuffer(size_t cnt)
{
    auto buffer = new de::MutableBuffer<R>(cnt, true, cnt);

    R rec;
    for (size_t i = 0; i < cnt; i++) {
        rec.key = rand();
        rec.value = rand();

        if constexpr (de::WeightedRecordInterface<R>) {
            rec.weight = 1;
        }

        buffer->append(rec);
    }

    return buffer;
}

template <de::RecordInterface R>
static de::MutableBuffer<R> *create_test_mbuffer_tombstones(size_t cnt, size_t ts_cnt) 
{
    auto buffer = new de::MutableBuffer<R>(cnt, true, ts_cnt);

    std::vector<std::pair<uint64_t, uint32_t>> tombstones;

    R rec;
    for (size_t i = 0; i < cnt; i++) {
        rec.key = rand();
        rec.value = rand();

        if constexpr (de::WeightedRecordInterface<R>) {
            rec.weight = 1;
        }

        if (i < ts_cnt) {
            tombstones.push_back({rec.key, rec.value});
        }

        buffer->append(rec);
    }

    rec.set_tombstone();
    for (size_t i=0; i<ts_cnt; i++) {
        buffer->append(rec);
    }

    return buffer;
}

template <de::WeightedRecordInterface R>
static de::MutableBuffer<R> *create_weighted_mbuffer(size_t cnt)
{
    auto buffer = new de::MutableBuffer<R>(cnt, true, cnt);
    
    // Put in half of the count with weight one.
    for (uint32_t i=0; i< cnt / 2; i++) {
        buffer->append(R {1, i, 2});
    }

    // put in a quarter of the count with weight four.
    for (uint32_t i=0; i< cnt / 4; i++) {
        buffer->append(R {2, i, 4});
    }

    // the remaining quarter with weight eight.
    for (uint32_t i=0; i< cnt / 4; i++) {
        buffer->append(R {3, i, 8});
    }

    return buffer;
}

template <de::RecordInterface R>
static de::MutableBuffer<R> *create_double_seq_mbuffer(size_t cnt, bool ts=false) 
{
    auto buffer = new de::MutableBuffer<R>(cnt, true, cnt);

    for (size_t i = 0; i < cnt / 2; i++) { 
        R rec;
        rec.key = i;
        rec.value = i;
        if (ts) rec.set_tombstone();

        buffer->append(rec);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        R rec;
        rec.key = i;
        rec.value = i + 1;
        if (ts) rec.set_tombstone();

        buffer->append(rec);
    }

    return buffer;
}


