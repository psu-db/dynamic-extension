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

typedef de::Record<const char*, uint64_t> StringRec;

static std::string kjv_wordlist = "tests/data/kjv-wordlist.txt";
static std::string summa_wordlist = "tests/data/summa-wordlist.txt";

static std::vector<std::unique_ptr<char[]>> string_data;

[[maybe_unused]] static std::vector<StringRec> read_string_data(std::string fname, size_t n) {
    std::vector<StringRec> vec;
    vec.reserve(n);
    string_data.reserve(n);

    std::fstream file;
    file.open(fname, std::ios::in);

    for (size_t i=0; i<n; i++) {
        std::string line;
        if (!std::getline(file, line, '\n')) break;

        std::stringstream ls(line);
        std::string field;

        std::getline(ls, field, '\t');
        auto val = atol(field.c_str());
        std::getline(ls, field, '\n');

        char *c = strdup(field.c_str());

        string_data.push_back(std::unique_ptr<char[]>(c));

        StringRec r(string_data[string_data.size() -1].get(), val, field.size());
        
        vec.push_back(r);
    }

    return vec;
}


template <de::RecordInterface R> 
std::vector<R> strip_wrapping(std::vector<de::Wrapped<R>> vec) {
    std::vector<R> out(vec.size());
    for (uint32_t i=0; i<vec.size(); i++) {
        out[i] = vec[i].rec;
    }

    return out;
}

[[maybe_unused]] static bool initialize_test_file(std::string fname, size_t page_cnt)
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

[[maybe_unused]] static bool roughly_equal(int n1, int n2, size_t mag, double epsilon) {
    return ((double) std::abs(n1 - n2) / (double) mag) < epsilon;
}

template <de::RecordInterface R>
static de::MutableBuffer<R> *create_test_mbuffer(size_t cnt)
{
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

    if constexpr (de::KVPInterface<R>){
        if constexpr (std::is_same_v<decltype(R::key), const char*>){
            auto records = read_string_data(kjv_wordlist, cnt);
            for (size_t i=0; i<cnt; i++) {
                buffer->append(records[i]);
            }
        } else {
            for (size_t i = 0; i < cnt; i++) {
                if constexpr (de::WeightedRecordInterface<R>) {
                    buffer->append({(uint64_t) rand(), (uint32_t) rand(), 1});
                } else {
                    buffer->append({(uint64_t) rand(), (uint32_t) rand()});
                }
            }
        }
    } else if constexpr (de::NDRecordInterface<R>) {
        for (size_t i=0; i<cnt; i++) {
            buffer->append({(uint64_t) rand(), (uint64_t) rand()});
        }
    } 

    return buffer;
}

template <de::RecordInterface R>
static de::MutableBuffer<R> *create_sequential_mbuffer(size_t start, size_t stop)
{
    size_t cnt = stop - start;
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

    for (uint32_t i=start; i<stop; i++) {

        if constexpr (de::WeightedRecordInterface<R>) {
            buffer->append({i, i, 1});
        } else {
            buffer->append({i, i});
        }
    }

    return buffer;
}

/*
template <de::KVPInterface R>
static de::MutableBuffer<R> *create_test_mbuffer_tombstones(size_t cnt, size_t ts_cnt) 
{
    auto buffer = new de::MutableBuffer<R>(cnt/2, cnt);

    std::vector<std::pair<uint64_t, uint32_t>> tombstones;

    R rec;
    for (size_t i = 0; i < cnt; i++) {
        if constexpr (de::WeightedRecordInterface<R>) {
            rec = {rand(), rand(), 1};
        } else {
            rec = {rand(), rand(), 1};
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
*/

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

    for (uint32_t i = 0; i < cnt / 2; i++) { 
        buffer->append({i, i}, ts);
    }

    for (uint32_t i = 0; i < cnt / 2; i++) {
        buffer->append({i, i+1}, ts);
    }

    return buffer;
}


