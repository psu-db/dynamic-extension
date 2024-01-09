/*
 * include/framework/structure/MutableBuffer.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                    Dong Xie <dongx@psu.edu>
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
#include "framework/structure/BufferView.h"

using psudb::CACHELINE_SIZE;

namespace de {

template <RecordInterface R>
class MutableBuffer {
    friend class BufferView<R>;
public:
    MutableBuffer(size_t low_watermark, size_t high_watermark, size_t capacity=0) 
    : m_lwm(low_watermark), m_hwm(high_watermark), m_cap(capacity), m_head(0), m_tail(0) {
        /* 
         * default capacity is twice the high water mark, to account for the worst-case
         * memory requirements.
         */
        if (m_cap == 0) {
            m_cap = m_hwm * 2;
        }

        m_data = (Wrapped<R> *) psudb::sf_aligned_alloc(CACHELINE_SIZE, m_cap * sizeof(Wrapped<R>));
        
        // FIXME: need to figure out how to detail with tombstones at some point...
        m_tombstone_filter = new psudb::BloomFilter<R>(BF_FPR, m_hwm, BF_HASH_FUNCS);
    }

    ~MutableBuffer() {
        assert(m_refcnt.load() == 0);

        if (m_data) free(m_data);
        if (m_tombstone_filter) delete m_tombstone_filter;
    }

    template <typename R_ = R>
    int append(const R &rec, bool tombstone=false) {
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

        return 1;     
    }

    bool truncate() {
        m_tombstonecnt.store(0);
        m_tail.store(0);
        if (m_tombstone_filter) m_tombstone_filter->clear();

        return true;
    }

    size_t get_record_count() {
        return (m_tail - m_head) % m_cap;
    }
    
    size_t get_capacity() {
        return m_cap;
    }

    bool is_full() {
        return (m_tail % m_cap) >= m_hwm;
    }

    size_t get_tombstone_count() {
        return m_tombstonecnt.load();
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

    bool check_tombstone(const R& rec) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(rec)) return false;

        for (size_t i=0; i<get_record_count(); i++) {
            if (m_data[to_idx(i)].rec == rec && m_data[to_idx(i)].is_tombstone()) {
                return true;
            }
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
        // FIXME: tombstone capacity needs figured out again
        return m_cap;
    }

    /*
     * Concurrency-related operations
     */
    bool take_reference() {
        m_refcnt.fetch_add(1);
        return true;
    }

    bool release_reference() {
        assert(m_refcnt > 0);
        m_refcnt.fetch_add(-1);
        return true;
    }

    size_t get_reference_count() {
        return m_refcnt.load();
    }

private:
    int64_t try_advance_tail() {
        int64_t new_tail = m_tail.fetch_add(1) % m_cap;

        if (new_tail < m_hwm) {
            return new_tail;
        }

        m_tail.fetch_add(-1);
        return -1;
    }

    size_t to_idx(size_t i) {
        return (m_head + i) % m_cap;
    }

    size_t m_cap;

    size_t m_lwm;
    size_t m_hwm;
    
    alignas(64) std::atomic<size_t> m_tail;
    alignas(64) std::atomic<size_t> m_head;
    
    Wrapped<R>* m_data;

    psudb::BloomFilter<R>* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    alignas(64) std::atomic<size_t> m_refcnt;
};

}
