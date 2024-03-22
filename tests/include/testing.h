/*
 * tests/testing.h
 *
 * Unit test utility functions/definitions
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include <string>

#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <sstream>

#include "util/types.h"
#include "psu-util/alignment.h"
#include "framework/structure/MutableBuffer.h"
#include "framework/interface/Record.h"

typedef de::WeightedRecord<uint64_t, uint32_t, uint64_t> WRec;
typedef de::Record<uint64_t, uint32_t> Rec;
typedef de::EuclidPoint<uint64_t> PRec;

typedef de::Record<std::string, uint64_t> StringRec;

std::string kjv_wordlist = "tests/data/kjv-wordlist.txt";
std::string summa_wordlist = "tests/data/summa-wordlist.txt";

static std::vector<StringRec> read_string_data(std::string fname, size_t n) {
    std::vector<StringRec> vec;
    vec.reserve(n);

    std::fstream file;
    file.open(fname, std::ios::in);

    for (size_t i=0; i<n; i++) {
        std::string line;
        if (!std::getline(file, line, '\n')) break;

        std::stringstream ls(line);
        StringRec r;
        std::string field;

        std::getline(ls, field, '\t');
        r.value = atol(field.c_str());
        std::getline(ls, field, '\n');
        r.key = std::string(field);
        
        vec.push_back(r);
    }

    return vec;
}


template <de::RecordInterface R> 
std::vector<R> strip_wrapping(std::vector<de::Wrapped<R>> vec) {
    std::vector<R> out(vec.size());
    for (size_t i=0; i<vec.size(); i++) {
        out[i] = vec[i].rec;
    }

    return out;
}

static bool initialize_test_file(std::string fname, size_t page_cnt)
{
    auto flags = O_RDWR | O_CREAT | O_TRUNC;
    mode_t mode = 0640;
    char *page = nullptr;

    int fd = open(fname.c_str(), flags, mode);
    if (fd == -1) {
        goto error;
    }

    page = (char *) aligned_alloc(psudb::SECTOR_SIZE, psudb::PAGE_SIZE);
    if (!page) {
        goto error_opened;
    }

    for (size_t i=0; i<=page_cnt; i++) {
        *((int *) page) = i;
        if (write(fd, page, psudb::PAGE_SIZE) == -1) {
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
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

    R rec;
    if constexpr (de::KVPInterface<R>){
        if constexpr (std::is_same_v<decltype(R::key), std::string>) {
            auto records = read_string_data(kjv_wordlist, cnt);
            for (size_t i=0; i<cnt; i++) {
                if constexpr (de::WeightedRecordInterface<R>) {
                    rec.weight = 1;
                }

                buffer->append(records[i]);
            }
        } else {
            for (size_t i = 0; i < cnt; i++) {
                rec.key = rand();
                rec.value = rand();

                if constexpr (de::WeightedRecordInterface<R>) {
                    rec.weight = 1;
                }

                buffer->append(rec);
            }
        }
    } else if constexpr (de::NDRecordInterface<R>) {
        for (size_t i=0; i<cnt; i++) {
            uint64_t a = rand();
            uint64_t b = rand();
            buffer->append({a, b});
        }
    } 

    return buffer;
}

template <de::RecordInterface R>
static de::MutableBuffer<R> *create_sequential_mbuffer(size_t start, size_t stop)
{
    size_t cnt = stop - start;
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

    for (size_t i=start; i<stop; i++) {
        R rec;
        if constexpr (de::KVPInterface<R>) {
            rec.key = i;
            rec.value = i;
        } else if constexpr (de::NDRecordInterface<R>) {
            rec = {i, i};
        }

        if constexpr (de::WeightedRecordInterface<R>) {
            rec.weight = 1;
        }

        buffer->append(rec);
    }

    return buffer;
}

template <de::KVPInterface R>
static de::MutableBuffer<R> *create_test_mbuffer_tombstones(size_t cnt, size_t ts_cnt) 
{
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

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

template <typename R>
requires de::WeightedRecordInterface<R> && de::KVPInterface<R>
static de::MutableBuffer<R> *create_weighted_mbuffer(size_t cnt)
{
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);
    
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

template <de::KVPInterface R>
static de::MutableBuffer<R> *create_double_seq_mbuffer(size_t cnt, bool ts=false) 
{
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

    for (size_t i = 0; i < cnt / 2; i++) { 
        R rec;
        rec.key = i;
        rec.value = i;

        buffer->append(rec, ts);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        R rec;
        rec.key = i;
        rec.value = i + 1;

        buffer->append(rec, ts);
    }

    return buffer;
}


