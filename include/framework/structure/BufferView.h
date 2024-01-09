/*
 * include/framework/structure/BufferView.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
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
#include "framework/interface/Query.h"

namespace de {

template <RecordInterface R>
class BufferView {
public:
    BufferView() = default;

    BufferView(const Wrapped<R> *buffer, size_t head, size_t tail, psudb::BloomFilter<R> *filter) 
        : m_buffer(buffer), m_head(head), m_tail(tail), m_tombstone_filter(filter) {}

    ~BufferView() = default;

    bool check_tombstone(const R& rec) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(rec)) return false;

        for (size_t i=0; i<get_record_count(); i++) {
            if (m_buffer[to_idx(i)].rec == rec && m_buffer[to_idx(i)].is_tombstone()) {
                return true;
            }
        }

        return false;
    }

    size_t get_record_count() {
        return m_tail - m_head;
    }
    
    size_t get_tombstone_count() {
        // FIXME: tombstone count
        return 0;
    }

    Wrapped<R> *get(size_t i) {
        assert(i < get_record_count());
        return m_buffer + to_idx(i);
    }

    void copy_to_buffer(byte *buffer) {
        memcpy(buffer, m_buffer, get_record_count() * sizeof(Wrapped<R>));
    }

private:
    const Wrapped<R>* m_buffer;
    size_t m_head;
    size_t m_tail;
    psudb::BloomFilter<R> *m_tombstone_filter;

    size_t to_idx(size_t i) {
        return (m_head + i) % m_buffer->get_capacity();
    }
};

}
