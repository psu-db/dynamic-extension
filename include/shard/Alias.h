/*
 * include/shard/Alias.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around the psudb::Alias Walker's Alias
 * structure. Designed to be used along side the WSS
 * query in include/query/wss.h
 *
 */
#pragma once

#include <vector>
#include <cassert>

#include "framework/ShardRequirements.h"

#include "psu-ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "psu-ds/Alias.h"
#include "psu-ds/BloomFilter.h"
#include "util/bf_config.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;

namespace de {

static thread_local size_t wss_cancelations = 0;

template <WeightedRecordInterface R>
class Alias {
private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:
    Alias(BufferView<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_alias(nullptr), m_bf(nullptr) {

        m_alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);

        m_bf = new BloomFilter<R>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        std::sort(base, stop, std::less<Wrapped<R>>());

        std::vector<W> weights;

        while (base < stop) {
            if (!(base->is_tombstone()) && (base + 1) < stop) {
                if (base->rec == (base + 1)->rec && (base + 1)->is_tombstone()) {
                    base += 2;
                    wss_cancelations++;
                    continue;
                }
            } else if (base->is_deleted()) {
                base += 1;
                continue;
            }

            // FIXME: this shouldn't be necessary, but the tagged record
            // bypass doesn't seem to be working on this code-path, so this
            // ensures that tagged records from the buffer are able to be
            // dropped, eventually. It should only need to be &= 1
            base->header &= 3;
            m_data[m_reccnt++] = *base;
            m_total_weight+= base->rec.weight;
            weights.push_back(base->rec.weight);

            if (m_bf && base->is_tombstone()) {
                m_tombstone_cnt++;
                m_bf->insert(base->rec);
            }
            
            base++;
        }

        if (m_reccnt > 0) {
            build_alias_structure(weights);
        }
    }

    Alias(std::vector<Alias*> &shards)
    : m_reccnt(0), m_tombstone_cnt(0), m_total_weight(0), m_alias(nullptr), m_bf(nullptr) {
        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(shards.size());

        PriorityQueue<Wrapped<R>> pq(shards.size());

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        
        for (size_t i = 0; i < shards.size(); ++i) {
            if (shards[i]) {
                auto base = shards[i]->get_data();
                cursors.emplace_back(Cursor{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
                attemp_reccnt += shards[i]->get_record_count();
                tombstone_count += shards[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);

        m_alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);

        std::vector<W> weights;
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor<Wrapped<R>>(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor<Wrapped<R>>(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    m_total_weight += cursor.ptr->rec.weight;
                    weights.push_back(cursor.ptr->rec.weight);
                    if (m_bf && cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        if (m_bf) m_bf->insert(cursor.ptr->rec);
                    }
                }
                pq.pop();
                
                if (advance_cursor<Wrapped<R>>(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            build_alias_structure(weights);
        }
   }

    ~Alias() {
        if (m_data) free(m_data);
        if (m_alias) delete m_alias;
        if (m_bf) delete m_bf;

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
        return m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        return 0;
    }

    W get_total_weight() {
        return m_total_weight;
    }

    size_t get_weighted_sample(gsl_rng *rng) const {
        return m_alias->get(rng);
    }

    size_t get_lower_bound(const K& key) const {
        size_t min = 0;
        size_t max = m_reccnt - 1;

        const char * record_key;
        while (min < max) {
            size_t mid = (min + max) / 2;

            if (key > m_data[mid].rec.key) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }

        return min;
    }

private:

    void build_alias_structure(std::vector<W> &weights) {

        // normalize the weights vector
        std::vector<double> norm_weights(weights.size());

        for (size_t i=0; i<weights.size(); i++) {
            norm_weights[i] = (double) weights[i] / (double) m_total_weight;
        }

        // build the alias structure
        m_alias = new psudb::Alias(norm_weights);
    }

    Wrapped<R>* m_data;
    psudb::Alias *m_alias;
    W m_total_weight;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_group_size;
    size_t m_alloc_size;
    BloomFilter<R> *m_bf;
};
}
