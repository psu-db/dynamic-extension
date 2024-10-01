/*
 * include/framework/structure/MutableBuffer.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * NOTE: Concerning the tombstone count. One possible approach
 * would be to track the number of tombstones below and above the
 * low water mark--this would be straightforward to do. Then, if we
 * *require* that the head only advance up to the LWM, we can get a
 * correct view on the number of tombstones in the active buffer at
 * any point in time, and the BufferView will have a pretty good
 * approximation as well (potentially with a few extra if new inserts
 * happen between when the tail pointer and tombstone count are fetched)
 *
 */
#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <immintrin.h>

#include "framework/interface/Record.h"
#include "framework/structure/BufferView.h"
#include "psu-ds/BloomFilter.h"
#include "psu-util/alignment.h"
#include "util/bf_config.h"

namespace de {

template <RecordInterface R> class MutableBuffer {
  friend class BufferView<R>;

  struct buffer_head {
    size_t head_idx;
    size_t refcnt;
  };

public:
  MutableBuffer(size_t low_watermark, size_t high_watermark,
                size_t capacity = 0)
      : m_lwm(low_watermark), m_hwm(high_watermark),
        m_cap((capacity == 0) ? 2 * high_watermark : capacity), m_tail(0),
        m_head({0, 0}), m_old_head({high_watermark, 0}),
        m_data(new Wrapped<R>[m_cap]()),
        m_tombstone_filter(
            new psudb::BloomFilter<R>(BF_FPR, m_hwm, BF_HASH_FUNCS)),
        m_tscnt(0), m_old_tscnt(0), m_active_head_advance(false) {
    assert(m_cap > m_hwm);
    assert(m_hwm >= m_lwm);
  }

  ~MutableBuffer() {
    delete[] m_data;
    delete m_tombstone_filter;
  }

  int append(const R &rec, bool tombstone = false) {
    int32_t tail = 0;
    if ((tail = try_advance_tail()) == -1) {
      return 0;
    }

    Wrapped<R> wrec;
    wrec.rec = rec;
    wrec.header = 0;
    if (tombstone)
      wrec.set_tombstone();

    // FIXME: because of the mod, it isn't correct to use `pos`
    //        as the ordering timestamp in the header anymore.
    size_t pos = tail % m_cap;

    m_data[pos] = wrec;
    m_data[pos].set_timestamp(pos);

    if (tombstone) {
      m_tscnt.fetch_add(1);
      if (m_tombstone_filter)
        m_tombstone_filter->insert(rec);
    }

    m_data[pos].set_visible();

    return 1;
  }

  bool truncate() {
    m_tscnt.store(0);
    m_tail.store(0);
    if (m_tombstone_filter)
      m_tombstone_filter->clear();

    return true;
  }

  size_t get_record_count() { return m_tail.load() - m_head.load().head_idx; }

  size_t get_capacity() { return m_cap; }

  bool is_full() { return get_record_count() >= m_hwm; }

  bool is_at_low_watermark() { return get_record_count() >= m_lwm; }

  size_t get_tombstone_count() { return m_tscnt.load(); }

  bool delete_record(const R &rec) {
    return get_buffer_view().delete_record(rec);
  }

  bool check_tombstone(const R &rec) {
    return get_buffer_view().check_tombstone(rec);
  }

  size_t get_memory_usage() { return m_cap * sizeof(Wrapped<R>); }

  size_t get_aux_memory_usage() {
    return m_tombstone_filter->get_memory_usage();
  }

  BufferView<R> get_buffer_view(size_t target_head) {
    size_t head = get_head(target_head);
    auto f = std::bind(release_head_reference, (void *)this, head);

    return BufferView<R>(m_data, m_cap, head, m_tail.load(), m_tscnt.load(),
                         m_tombstone_filter, f);
  }

  BufferView<R> get_buffer_view() {
    size_t head = get_head(m_head.load().head_idx);
    auto f = std::bind(release_head_reference, (void *)this, head);

    return BufferView<R>(m_data, m_cap, head, m_tail.load(), m_tscnt.load(),
                         m_tombstone_filter, f);
  }

  /*
   * Advance the buffer following a reconstruction. Move current
   * head and head_refcnt into old_head and old_head_refcnt, then
   * assign new_head to old_head.
   */
  bool advance_head(size_t new_head) {
    assert(new_head > m_head.load().head_idx);
    assert(new_head <= m_tail.load());

    /* refuse to advance head while there is an old with one references */
    if (m_old_head.load().refcnt > 0) {
      // fprintf(stderr, "[W]: Refusing to advance head due to remaining
      // reference counts\n");
      return false;
    }

    m_active_head_advance.store(true);

    buffer_head new_hd = {new_head, 0};
    buffer_head cur_hd;

    /* replace current head with new head */
    do {
      cur_hd = m_head.load();
    } while (!m_head.compare_exchange_strong(cur_hd, new_hd));

    /* move the current head into the old head */
    m_old_head.store(cur_hd);

    m_active_head_advance.store(false);
    return true;
  }

  /*
   * FIXME: If target_head does not match *either* the old_head or the
   * current_head, this routine will loop infinitely.
   */
  size_t get_head(size_t target_head) {
    buffer_head cur_hd, new_hd;
    bool head_acquired = false;

    do {
      if (m_old_head.load().head_idx == target_head) {
        cur_hd = m_old_head.load();
        cur_hd.head_idx = target_head;
        new_hd = {cur_hd.head_idx, cur_hd.refcnt + 1};
        head_acquired = m_old_head.compare_exchange_strong(cur_hd, new_hd);
      } else if (m_head.load().head_idx == target_head) {
        cur_hd = m_head.load();
        cur_hd.head_idx = target_head;
        new_hd = {cur_hd.head_idx, cur_hd.refcnt + 1};
        head_acquired = m_head.compare_exchange_strong(cur_hd, new_hd);
      }
    } while (!head_acquired);

    return new_hd.head_idx;
  }

  void set_low_watermark(size_t lwm) {
    assert(lwm < m_hwm);
    m_lwm = lwm;
  }

  size_t get_low_watermark() { return m_lwm; }

  void set_high_watermark(size_t hwm) {
    assert(hwm > m_lwm);
    assert(hwm < m_cap);
    m_hwm = hwm;
  }

  size_t get_high_watermark() { return m_hwm; }

  size_t get_tail() { return m_tail.load(); }

  /*
   * Note: this returns the available physical storage capacity,
   * *not* now many more records can be inserted before the
   * HWM is reached. It considers the old_head to be "free"
   * when it has no remaining references. This should be true,
   * but a buggy framework implementation may violate the
   * assumption.
   */
  size_t get_available_capacity() {
    if (m_old_head.load().refcnt == 0) {
      return m_cap - (m_tail.load() - m_head.load().head_idx);
    }

    return m_cap - (m_tail.load() - m_old_head.load().head_idx);
  }

private:
  int64_t try_advance_tail() {
    size_t old_value = m_tail.load();

    /* if full, fail to advance the tail */
    if (old_value - m_head.load().head_idx >= m_hwm) {
      return -1;
    }

    while (!m_tail.compare_exchange_strong(old_value, old_value + 1)) {
      /* if full, stop trying and fail to advance the tail */
      if (m_tail.load() >= m_hwm) {
        return -1;
      }

      _mm_pause();
    }

    return old_value;
  }

  size_t to_idx(size_t i, size_t head) { return (head + i) % m_cap; }

  static void release_head_reference(void *buff, size_t head) {
    MutableBuffer<R> *buffer = (MutableBuffer<R> *)buff;

    buffer_head cur_hd, new_hd;
    do {
      if (buffer->m_old_head.load().head_idx == head) {
        cur_hd = buffer->m_old_head;
        if (cur_hd.refcnt == 0)
          continue;
        new_hd = {cur_hd.head_idx, cur_hd.refcnt - 1};
        if (buffer->m_old_head.compare_exchange_strong(cur_hd, new_hd)) {
          break;
        }
      } else {
        cur_hd = buffer->m_head;
        if (cur_hd.refcnt == 0)
          continue;
        new_hd = {cur_hd.head_idx, cur_hd.refcnt - 1};

        if (buffer->m_head.compare_exchange_strong(cur_hd, new_hd)) {
          break;
        }
      }
      _mm_pause();
    } while (true);
  }

  size_t m_lwm;
  size_t m_hwm;
  size_t m_cap;

  alignas(64) std::atomic<size_t> m_tail;

  alignas(64) std::atomic<buffer_head> m_head;
  alignas(64) std::atomic<buffer_head> m_old_head;

  Wrapped<R> *m_data;
  psudb::BloomFilter<R> *m_tombstone_filter;
  alignas(64) std::atomic<size_t> m_tscnt;
  size_t m_old_tscnt;

  alignas(64) std::atomic<bool> m_active_head_advance;
};

} // namespace de
