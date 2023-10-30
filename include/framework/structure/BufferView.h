/*
 * include/framework/structure/BufferView.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <atomic>
#include <condition_variable>
#include <cassert>
#include <numeric>
#include <algorithm>
#include <type_traits>

#include "psu-util/alignment.h"
#include "util/bf_config.h"
#include "psu-ds/BloomFilter.h"
#include "psu-ds/Alias.h"
#include "psu-util/timer.h"
#include "framework/interface/Record.h"
#include "framework/structure/MutableBuffer.h"
#include "framework/interface/Query.h"

namespace de {

template <RecordInterface R, QueryInterface Q>
class BufferView {
    typedef MutableBuffer<R> Buffer;
public:
    BufferView() = default;

    BufferView(std::vector<Buffer*> buffers) 
        : m_buffers(buffers)
        , m_cutoff(buffers[buffers.size()-1]->get_record_count())
    {}

    ~BufferView() = default;

    bool delete_record(const R& rec) {
        auto res = false;
        for (auto buf : m_buffers) {
            res = buf->delete_record(rec);
            if (res) return true;
        }
        return false;
    }

    bool check_tombstone(const R& rec) {
        auto res = false;
        for (auto buf : m_buffers) {
            res = buf->check_tombstone(rec);
            if (res) return true;
        }
        return false;
    }

    size_t get_record_count() {
        size_t reccnt = 0;
        for (auto buf : m_buffers) {
            reccnt += buf->get_record_count();
        }
        return reccnt;
    }
    
    size_t get_capacity() {
        return m_buffers[0]->get_capacity();
    }

    bool is_full() {
        return m_buffers[m_buffers.size() - 1]->is_full();
    }

    size_t get_tombstone_count() {
        size_t tscnt = 0;
        for (auto buf : m_buffers) {
            tscnt += buf->get_tombstone_count();
        }
        return tscnt;
    }

    size_t get_memory_usage() {
        size_t mem = 0;
        for (auto buf : m_buffers) {
            mem += buf->get_memory_usage();
        }
        return mem;
    }

    size_t get_aux_memory_usage() {
        size_t mem = 0;
        for (auto buf : m_buffers) {
            mem += buf->get_aux_memory_usage();
        }
        return mem;
    }

    size_t get_tombstone_capacity() {
        return m_buffers[0]->get_tombstone_capacity();
    }

    std::vector<void *> get_query_states(void *parms) {
        std::vector<void *> states;

        for (auto buf : m_buffers) {
            states.push_back(Q::get_buffer_query_state(buf, parms));
        }

        return states;
    }

    std::vector<Buffer *> &get_buffers() {
        return m_buffers;
    }

    size_t size() {
        return m_buffers.size();
    }

private:
    std::vector<Buffer *> m_buffers;
    size_t m_cutoff;
};

}
