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
#include <cassert>

#include "psu-util/alignment.h"
#include "util/bf_config.h"
#include "psu-ds/BloomFilter.h"
#include "framework/interface/Record.h"
#include "framework/structure/BufferView.h"

using psudb::CACHELINE_SIZE;

namespace de {

template <RecordInterface R>
class MutableBuffer {
    friend class BufferView<R>;
public:
    MutableBuffer(size_t low_watermark, size_t high_watermark, size_t capacity=0) 
    : m_lwm(low_watermark), m_hwm(high_watermark), m_cap(capacity), m_head(0), m_tail(0),
    m_old_head(0), m_head_refcnt(0), m_old_head_refcnt(0) {
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
        free(m_data);
        delete m_tombstone_filter;
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
        return get_record_count() >= m_hwm;
    }

    bool is_at_low_watermark() {
        return (m_tail % m_cap) > m_lwm;
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
        return m_cap * sizeof(Wrapped<R>);
    }

    size_t get_aux_memory_usage() {
        return m_tombstone_filter->get_memory_usage();
    }

    BufferView<R> get_buffer_view() {
        m_head_refcnt.fetch_add(1);
        return BufferView(m_data, m_head, m_tail.load(), m_tombstone_filter, (void*) this, release_head_reference);
    }

    /*
     * Advance the buffer following a reconstruction. Move current
     * head and head_refcnt into old_head and old_head_refcnt, then
     * assign new_head to old_head.
     */
    void advance_head(size_t new_head) {
        assert(new_head > m_head.load());
        assert(new_head <= m_tail.load());
        assert(m_old_head_refcnt == 0);

        /*
         * the order here is very important. We first store zero to the 
         * old_refcnt (should be zero anyway). Then we move the current 
         * head to old head. At this point, any new buffer views should
         * increment the old head refcnt, so no new references to the 
         * current head will be taken. Then we add the current head 
         * refcnt to this. This is to ensure that no references get 
         * dropped. Only after this do we change to the new head
         */
        m_old_head_refcnt.store(0);
        m_old_head.store(m_head.load());
        m_old_head_refcnt.fetch_add(m_head_refcnt);

        m_head_refcnt.store(0);
        m_head.store(new_head);
    }

    void set_low_watermark(size_t lwm) {
        assert(lwm < m_hwm);
        m_lwm = lwm;
    }

    size_t get_low_watermark() {
        return m_lwm;
    }

    void set_high_watermark(size_t hwm) {
        assert(hwm > m_lwm);
        assert(hwm < m_cap);
        m_hwm = hwm;
    }

    size_t get_high_watermark() {
        return m_hwm;
    }

    size_t get_tail() {
        return m_tail.load();
    }

    /*
     * Note: this returns the available physical storage capacity,
     * *not* now many more records can be inserted before the
     * HWM is reached.
     */
    size_t get_available_capacity() {
        return m_cap - (m_tail.load() - m_old_head.load());
    }

private:
    int64_t try_advance_tail() {
        size_t old_value = m_tail.load();

        /* if full, fail to advance the tail */
        if (old_value >= m_hwm) {
            return -1;
        }

        while (!m_tail.compare_exchange_strong(old_value, old_value+1)) {
            /* if full, stop trying and fail to advance the tail */
            if (m_tail.load() >= m_hwm) {
                return -1;
            }
        }

        return old_value;
    }

    size_t to_idx(size_t i) {
        return (m_head + i) % m_cap;
    }

    static void release_head_reference(void *buff, size_t head) {
        MutableBuffer<R> *buffer = (MutableBuffer<R> *) buff;

        if (head == buffer->m_head.load()) {
            buffer->m_head_refcnt.fetch_sub(1);
        } else if (head == buffer->m_old_head.load()) {
            buffer->m_old_head_refcnt.fetch_sub(1);
            /* 
             * if the old head refcnt drops to 0, free
             * the records by setting old_head = head
             */
            if (buffer->m_old_head_refcnt.load() == 0) {
                buffer->m_old_head.store(buffer->m_head);   
            }
        }
    }

    size_t m_lwm;
    size_t m_hwm;
    size_t m_cap;
    
    alignas(64) std::atomic<size_t> m_tail;

    alignas(64) std::atomic<size_t> m_head;
    alignas(64) std::atomic<size_t> m_head_refcnt;

    alignas(64) std::atomic<size_t> m_old_head;
    alignas(64) std::atomic<size_t> m_old_head_refcnt;
    
    Wrapped<R>* m_data;
    psudb::BloomFilter<R>* m_tombstone_filter;
    alignas(64) std::atomic<size_t> m_tombstonecnt;
};

}
