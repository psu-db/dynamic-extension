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
#include <utility>

#include "psu-util/alignment.h"
#include "psu-ds/BloomFilter.h"
#include "framework/interface/Record.h"

namespace de {

typedef std::_Bind<void (*(void*, long unsigned int))(void*, long unsigned int)> ReleaseFunction;

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
        for (size_t i=0; i<get_record_count(); i++) {
            if (m_data[to_idx(i)].rec == rec) {
                m_data[to_idx(i)].set_delete();
                return true;
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
        memcpy(buffer, (std::byte*) (m_data + (m_head % m_cap)), get_record_count() * sizeof(Wrapped<R>));
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
    size_t m_cap;
    size_t m_approx_ts_cnt;
    psudb::BloomFilter<R> *m_tombstone_filter;
    bool m_active;

    size_t to_idx(size_t i) {
        return (m_head + i) % m_cap;
    }
};

}
