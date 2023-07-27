/*
 * include/framework/MutableBuffer.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <atomic>
#include <cassert>
#include <numeric>
#include <algorithm>
#include <type_traits>

#include "util/base.h"
#include "util/bf_config.h"
#include "ds/BloomFilter.h"
#include "ds/Alias.h"
#include "util/timer.h"
#include "framework/RecordInterface.h"

namespace de {


template <typename R>
class MutableBuffer {
public:
    MutableBuffer(size_t capacity, size_t max_tombstone_cap)
    : m_cap(capacity), m_tombstone_cap(max_tombstone_cap), m_reccnt(0)
    , m_tombstonecnt(0), m_weight(0), m_max_weight(0) {
        auto len = capacity * sizeof(Wrapped<R>);
        size_t aligned_buffersize = len + (CACHELINE_SIZE - (len % CACHELINE_SIZE));
        m_data = (Wrapped<R>*) aligned_alloc(CACHELINE_SIZE, aligned_buffersize);
        m_tombstone_filter = nullptr;
        if (max_tombstone_cap > 0) {
            m_tombstone_filter = new BloomFilter<R>(BF_FPR, max_tombstone_cap, BF_HASH_FUNCS);
        }
    }

    ~MutableBuffer() {
        if (m_data) free(m_data);
        if (m_tombstone_filter) delete m_tombstone_filter;
    }

    template <typename R_ = R>
    int append(const R &rec, bool tombstone=false) {
        if (tombstone && m_tombstonecnt + 1 > m_tombstone_cap) return 0;

        int32_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;

        Wrapped<R> wrec;
        wrec.rec = rec;
        wrec.header = 0;
        if (tombstone) wrec.set_tombstone();

        m_data[pos] = wrec;
        m_data[pos].header |= (pos << 2);

        if (tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(rec);
        }

        m_weight.store(m_weight.load() + 1);

        return 1;     
    }

    bool truncate() {
        m_tombstonecnt.store(0);
        m_reccnt.store(0);
        m_weight.store(0);
        m_max_weight.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        return true;
    }

    size_t get_record_count() {
        return m_reccnt;
    }
    
    size_t get_capacity() {
        return m_cap;
    }

    bool is_full() {
        return m_reccnt == m_cap;
    }

    size_t get_tombstone_count() {
        return m_tombstonecnt.load();
    }

    bool delete_record(const R& rec) {
        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].rec == rec) {
                m_data[offset].set_delete();
                return true;
            }
            offset++;
        }

        return false;
    }

    bool check_tombstone(const R& rec) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(rec)) return false;

        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].rec == rec && m_data[offset].is_tombstone()) {
                return true;
            }
            offset++;;
        }
        return false;
    }

    size_t get_memory_usage() {
        return m_cap * sizeof(R);
    }

    size_t get_aux_memory_usage() {
        return m_tombstone_filter->get_memory_usage();
    }

    size_t get_tombstone_capacity() {
        return m_tombstone_cap;
    }

    double get_total_weight() {
        return m_weight.load();
    }

    Wrapped<R> *get_data() {
        return m_data;
    }

    double get_max_weight() {
        return m_max_weight;
    }

private:
    int32_t try_advance_tail() {
        size_t new_tail = m_reccnt.fetch_add(1);

        if (new_tail < m_cap) return new_tail;
        else return -1;
    }

    size_t m_cap;
    size_t m_tombstone_cap;
    
    Wrapped<R>* m_data;
    BloomFilter<R>* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<uint32_t> m_reccnt;
    alignas(64) std::atomic<double> m_weight;
    alignas(64) std::atomic<double> m_max_weight;
};

}
