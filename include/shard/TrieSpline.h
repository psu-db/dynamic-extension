/*
 * include/shard/TrieSpline.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the TrieSpline learned index.
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once


#include <vector>

#include "framework/ShardRequirements.h"
#include "ts/builder.h"
#include "psu-ds/BloomFilter.h"
#include "util/bf_config.h"
#include "util/SortedMerge.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::byte;

namespace de {

template <KVPInterface R, size_t E=1024>
class TrieSpline {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;

public:
    TrieSpline(BufferView<R> buffer)
        : m_data(nullptr)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_max_key(0)
        , m_min_key(0)
        , m_bf(new BloomFilter<R>(BF_FPR, buffer.get_tombstone_count(), BF_HASH_FUNCS))
    {
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

        auto res = sorted_array_from_bufferview(std::move(buffer), m_data, m_bf);
        m_reccnt = res.record_count;
        m_tombstone_cnt = res.tombstone_count;

        if (m_reccnt > 0) {
            m_min_key = m_data[0].rec.key;
            m_max_key = m_data[m_reccnt-1].rec.key;

            auto bldr = ts::Builder<K>(m_min_key, m_max_key, E);
            for (size_t i=0; i<m_reccnt; i++) {
                bldr.AddKey(m_data[i].rec.key);
            }

            m_ts = bldr.Finalize();
        }
    }

    TrieSpline(std::vector<TrieSpline*> &shards) 
        : m_data(nullptr)
        , m_reccnt(0)
        , m_tombstone_cnt(0)
        , m_alloc_size(0)
        , m_max_key(0)
        , m_min_key(0)
        , m_bf(nullptr)
    {
        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        auto cursors = build_cursor_vec<R, TrieSpline>(shards, &attemp_reccnt, &tombstone_count);
        
        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);
        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);

        auto res = sorted_array_merge<R>(cursors, m_data, m_bf);
        m_reccnt = res.record_count;
        m_tombstone_cnt = res.tombstone_count;

        if (m_reccnt > 0) {
            m_min_key = m_data[0].rec.key;
            m_max_key = m_data[m_reccnt-1].rec.key;

            auto bldr = ts::Builder<K>(m_min_key, m_max_key, E);
            for (size_t i=0; i<m_reccnt; i++) {
                bldr.AddKey(m_data[i].rec.key);
            }

            m_ts = bldr.Finalize();
        }
    }

    ~TrieSpline() {
        free(m_data);
        delete m_bf;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if (filter && !m_bf->lookup(rec)) {
            return nullptr;
        }

        size_t idx = get_lower_bound(rec.key);
        if (idx >= m_reccnt) {
            return nullptr;
        }

        while (idx < m_reccnt && m_data[idx].rec < rec) ++idx;

        if (m_data[idx].rec == rec) {
            return m_data + idx;
        }

        return nullptr;
    }

    Wrapped<R>* get_data() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    const Wrapped<R>* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx;
    }


    size_t get_memory_usage() {
        return m_ts.GetSize() + m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        return (m_bf) ? m_bf->memory_usage() : 0;
    }

    size_t get_lower_bound(const K& key) const {
        auto bound = m_ts.GetSearchBound(key);
        size_t idx = bound.begin;

        if (idx >= m_reccnt) {
            return m_reccnt;
        }

        // If the region to search is less than some pre-specified
        // amount, perform a linear scan to locate the record.
        if (bound.end - bound.begin < 256) {
            while (idx < bound.end && m_data[idx].rec.key < key) {
                idx++;
            }
        } else {
            // Otherwise, perform a binary search
            idx = bound.begin;
            size_t max = bound.end;

            while (idx < max) {
                size_t mid = (idx + max) / 2;
                if (key > m_data[mid].rec.key) {
                    idx = mid + 1;
                } else {
                    max = mid;
                }
            }
        }

        if (idx == m_reccnt) {
            return m_reccnt;
        }

        if (m_data[idx].rec.key > key && idx > 0 && m_data[idx-1].rec.key <= key) {
            return idx-1;
        }

        return idx;
    }

private:

    Wrapped<R>* m_data;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_alloc_size;
    K m_max_key;
    K m_min_key;
    ts::TrieSpline<K> m_ts;
    BloomFilter<R> *m_bf;
};
}
