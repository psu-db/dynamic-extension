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
#include <cassert>
#include <functional>

#include "psu-util/alignment.h"
#include "psu-ds/BloomFilter.h"
#include "framework/interface/Record.h"

namespace de {

typedef std::function<void(void*, size_t)> ReleaseFunction;

template <RecordInterface R>
class BufferView {
public:
    BufferView() = default;

    BufferView(const Wrapped<R> *buffer, size_t head, size_t tail, psudb::BloomFilter<R> *filter,
               void *parent_buffer, ReleaseFunction release) 
        : m_buffer(buffer)
        , m_release(release)
        , m_parent_buffer(parent_buffer)
        , m_head(head)
        , m_tail(tail)
        , m_tombstone_filter(filter) {}

    ~BufferView() {
        m_release(m_parent_buffer, m_head);
    }

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

    void copy_to_buffer(psudb::byte *buffer) {
        memcpy(buffer, (std::byte*) (m_buffer + m_head), get_record_count() * sizeof(Wrapped<R>));
    }

private:
    const Wrapped<R>* m_buffer;
    void *m_parent_buffer;
    ReleaseFunction m_release;
    size_t m_head;
    size_t m_tail;
    psudb::BloomFilter<R> *m_tombstone_filter;

    size_t to_idx(size_t i) {
        return (m_head + i) % m_buffer->get_capacity();
    }
};

}
