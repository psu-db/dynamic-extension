/*
 * include/framework/structure/BufferView.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 * TODO: This file is very poorly commented.
 */
#pragma once

#include <cstdlib>
#include <cassert>
#include <functional>
#include <utility>

#include "psu-util/alignment.h"
#include "psu-ds/BloomFilter.h"
#include "framework/interface/Record.h"

namespace de {

typedef std::function<void(void)> ReleaseFunction; 

template <RecordInterface R>
class BufferView {
public:
    BufferView() = default;

    /* 
     * the BufferView's lifetime is tightly linked to buffer versioning, and so
     * copying and assignment are disabled.
     */
    BufferView(const BufferView&) = delete;
    BufferView &operator=(BufferView &) = delete;

    BufferView(BufferView &&other) 
        : m_data(std::exchange(other.m_data, nullptr))
        , m_release(std::move(other.m_release))
        , m_head(std::exchange(other.m_head, 0))
        , m_tail(std::exchange(other.m_tail, 0))
        , m_start(std::exchange(other.m_start, 0))
        , m_stop(std::exchange(other.m_stop, 0))
        , m_cap(std::exchange(other.m_cap, 0))
        , m_approx_ts_cnt(std::exchange(other.m_approx_ts_cnt, 0))
        , m_tombstone_filter(std::exchange(other.m_tombstone_filter, nullptr))
        , m_active(std::exchange(other.m_active, false)) {}

    BufferView &operator=(BufferView &&other) = delete;


    BufferView(Wrapped<R> *buffer, size_t cap, size_t head, size_t tail, size_t tombstone_cnt, psudb::BloomFilter<R> *filter,
               ReleaseFunction release) 
        : m_data(buffer)
        , m_release(release)
        , m_head(head)
        , m_tail(tail)
        , m_start(m_head % cap)
        , m_stop(m_tail % cap)
        , m_cap(cap)
        , m_approx_ts_cnt(tombstone_cnt)
        , m_tombstone_filter(filter)
        , m_active(true) {}

    ~BufferView() {
        if (m_active) {
            m_release();
        }
    }

    bool check_tombstone(const R& rec) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(rec)) return false;

        for (size_t i=0; i<get_record_count(); i++) {
            if (m_data[to_idx(i)].rec == rec && m_data[to_idx(i)].is_tombstone()) {
                return true;
            }
        }

        return false;
    }

    bool delete_record(const R& rec) {
        if (m_start < m_stop) {
            for (size_t i=m_start; i<m_stop; i++) {
                if (m_data[i].rec == rec) {
                    m_data[i].set_delete();
                    return true;
                }
            }
        } else {
            for (size_t i=m_start; i<m_cap; i++) {
                if (m_data[i].rec == rec) {
                    m_data[i].set_delete();
                    return true;
                }
            }

            for (size_t i=0; i<m_stop; i++) {
                if (m_data[i].rec == rec) {
                    m_data[i].set_delete();
                    return true;
                }

            }

        }

        return false;
    }

    size_t get_record_count() {
        return m_tail - m_head;
    }
    
    /*
     * NOTE: This function returns an upper bound on the number
     *       of tombstones within the view. There may be less than
     *       this, due to synchronization issues during view creation.
     */
    size_t get_tombstone_count() {
        return m_approx_ts_cnt;
    }

    Wrapped<R> *get(size_t i) {
        assert(i < get_record_count());
        return m_data + to_idx(i);
    }

    void copy_to_buffer(psudb::byte *buffer) {
        /* check if the region to be copied circles back to start. If so, do it in two steps */
        if (m_start > m_stop) { 
            size_t split_idx = m_cap - m_start;

            memcpy(buffer, (std::byte*) (m_data + m_start), split_idx* sizeof(Wrapped<R>));
            memcpy(buffer + (split_idx * sizeof(Wrapped<R>)), (std::byte*) m_data, m_stop * sizeof(Wrapped<R>));
        } else {
            memcpy(buffer, (std::byte*) (m_data + m_start), get_record_count() * sizeof(Wrapped<R>));
        }
    }

    size_t get_tail() {
        return m_tail;
    }

    size_t get_head() {
        return m_head;
    }

private:
    Wrapped<R>* m_data;
    ReleaseFunction m_release;
    size_t m_head;
    size_t m_tail;
    size_t m_start;
    size_t m_stop;
    size_t m_cap;
    size_t m_approx_ts_cnt;
    psudb::BloomFilter<R> *m_tombstone_filter;
    bool m_active;

    size_t to_idx(size_t i) {
        size_t idx = (m_start + i >= m_cap) ? i = (m_cap - m_start) 
                                            : m_start + i;
        assert(idx < m_cap);
        return idx;
    }
};

}
