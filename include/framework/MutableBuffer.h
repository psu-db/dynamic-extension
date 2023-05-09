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

#include "util/base.h"
#include "util/bf_config.h"
#include "ds/BloomFilter.h"
#include "util/Record.h"
#include "ds/Alias.h"
#include "util/timer.h"

namespace de {


template <typename K, typename V, typename W=void>
class MutableBuffer {
public:
    MutableBuffer(size_t capacity, bool rej_sampling, size_t max_tombstone_cap, const gsl_rng* rng)
    : m_cap(capacity), m_tombstone_cap(max_tombstone_cap), m_reccnt(0)
    , m_tombstonecnt(0), m_weight(0), m_max_weight(0) {
        auto len = capacity * sizeof(Record<K, V, W>);
        size_t aligned_buffersize = len + (CACHELINE_SIZE - (len % CACHELINE_SIZE));
        m_data = (Record<K, V, W>*) std::aligned_alloc(CACHELINE_SIZE, aligned_buffersize);
        m_tombstone_filter = nullptr;
        if (max_tombstone_cap > 0) {
            assert(rng != nullptr);
            m_tombstone_filter = new BloomFilter(BF_FPR, max_tombstone_cap, BF_HASH_FUNCS, rng);
        }
    }

    ~MutableBuffer() {
        if (m_data) free(m_data);
        if (m_tombstone_filter) delete m_tombstone_filter;
    }

    int append(const K& key, const V& value, W weight = 1, bool is_tombstone = false) {
        if (is_tombstone && m_tombstonecnt + 1 > m_tombstone_cap) return 0;

        int32_t pos = 0;
        if ((pos = try_advance_tail()) == -1) return 0;

        if (is_tombstone) {
            weight = 0;
        }

        m_data[pos].key = key;
        m_data[pos].value = value;
        m_data[pos].header = ((pos << 2) | (is_tombstone ? 1 : 0));
        m_data[pos].weight = weight;

        if (is_tombstone) {
            m_tombstonecnt.fetch_add(1);
            if (m_tombstone_filter) m_tombstone_filter->insert(key);
        }

        double old_val, new_val;
        do {
            old_val = m_weight.load();
            new_val = old_val + weight;
        } while (!m_weight.compare_exchange_strong(old_val, new_val));


        double old = m_max_weight.load();
        while (old < weight) {
            m_max_weight.compare_exchange_strong(old, weight);
            old = m_max_weight.load();
        }

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

    Record<K, V, W>* sorted_output() {
        TIMER_INIT();
        TIMER_START();
        std::sort(m_data, m_data + m_reccnt.load(), memtable_record_cmp);
        TIMER_STOP();

        #ifdef INSTRUMENT_MERGING
        fprintf(stderr, "sort\t%ld\n", TIMER_RESULT());
        #endif
        return m_data;
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

    bool delete_record(const K& key, const V& val) {
        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].match(key, val, false)) {
                m_data[offset].set_delete_status();
                return true;
            }
            offset++;
        }

        return false;
    }

    bool check_tombstone(const K& key, const V& value) {
        if (m_tombstone_filter && !m_tombstone_filter->lookup(key)) return false;

        auto offset = 0;
        while (offset < m_reccnt.load()) {
            if (m_data[offset].match(key, value, true)) return true;
            offset++;;
        }
        return false;
    }

    const Record<K, V, W>* get_record_at(size_t idx) {
        return m_data + idx;
    }

    size_t get_memory_utilization() {
        return m_cap * sizeof(Record<K, V, W>);
    }

    size_t get_aux_memory_utilization() {
        return m_tombstone_filter->get_memory_utilization();
    }
    //
    // NOTE: This operation samples from records strictly between the upper and
    // lower bounds, not including them
    double get_sample_range(const K& lower, const K& upper, std::vector<Record<K, V, W> *> &records, 
                            Alias **alias, size_t *cutoff) {
      std::vector<double> weights;

      *cutoff = std::atomic_load(&m_reccnt) - 1;
      records.clear();
      double tot_weight = 0.0;
      for (size_t i = 0; i < (*cutoff) + 1; i++) {
        Record<K, V, W> *rec = m_data + i;

        if (rec->key >= lower && rec->key <= upper && !rec->is_tombstone() && !rec->get_delete_status()) {
          weights.push_back(rec->weight);
          records.push_back(rec);
          tot_weight += rec->weight;
        }
      }

      for (size_t i = 0; i < weights.size(); i++) {
        weights[i] = weights[i] / tot_weight;
      }

      *alias = new Alias(weights);

      return tot_weight;
    }

    // rejection sampling
    const Record<K, V, W> *get_sample(const K& lower, const K& upper, gsl_rng *rng) {
        size_t reccnt = m_reccnt.load();
        if (reccnt == 0) {
            return nullptr;
        }

        auto idx = (reccnt == 1) ? 0 : gsl_rng_uniform_int(rng, reccnt - 1);
        auto rec = get_record_at(idx);

        auto test = gsl_rng_uniform(rng) * m_max_weight.load();

        if (test > rec->weight) {
            return nullptr;
        }

        if (test <= rec->weight &&
          rec->key >= lower &&
          rec->key <= upper && 
          !rec->is_tombstone() && !rec->get_delete_status()) {

            return rec;
        }

        return nullptr;
    }

    size_t get_tombstone_capacity() {
        return m_tombstone_cap;
    }

    double get_total_weight() {
        return m_weight.load();
    }

private:
    int32_t try_advance_tail() {
        size_t new_tail = m_reccnt.fetch_add(1);

        if (new_tail < m_cap) return new_tail;
        else return -1;
    }

    size_t m_cap;
    //size_t m_buffersize;
    size_t m_tombstone_cap;
    
    //char* m_data;
    Record<K, V, W>* m_data;
    BloomFilter* m_tombstone_filter;

    alignas(64) std::atomic<size_t> m_tombstonecnt;
    //alignas(64) std::atomic<uint32_t> m_current_tail;
    alignas(64) std::atomic<uint32_t> m_reccnt;
    alignas(64) std::atomic<double> m_weight;
    alignas(64) std::atomic<double> m_max_weight;
};

}
