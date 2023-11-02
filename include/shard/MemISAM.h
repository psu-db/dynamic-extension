/*
 * include/shard/MemISAM.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>

#include "framework/ShardRequirements.h"

#include "util/bf_config.h"
#include "psu-ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "psu-util/timer.h"

using psudb::CACHELINE_SIZE;
using psudb::BloomFilter;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::Alias;

namespace de {

thread_local size_t mrun_cancelations = 0;

template <RecordInterface R>
class MemISAM {
private:
    friend class IRSQuery<R, true>;
    friend class IRSQuery<R, false>;
    friend class ISAMRangeQuery<R>;

typedef decltype(R::key) K;
typedef decltype(R::value) V;

constexpr static size_t inmem_isam_node_size = 256;
constexpr static size_t inmem_isam_fanout = inmem_isam_node_size / (sizeof(K) + sizeof(char*));

struct InMemISAMNode {
    K keys[inmem_isam_fanout];
    char* child[inmem_isam_fanout];
};

constexpr static size_t inmem_isam_leaf_fanout = inmem_isam_node_size / sizeof(R);
constexpr static size_t inmem_isam_node_keyskip = sizeof(K) * inmem_isam_fanout;

static_assert(sizeof(InMemISAMNode) == inmem_isam_node_size, "node size does not match");

public:
    MemISAM(MutableBuffer<R>* buffer)
    :m_reccnt(0), m_tombstone_cnt(0), m_isam_nodes(nullptr), m_deleted_cnt(0) {

        m_bf = new BloomFilter<R>(BF_FPR, buffer->get_tombstone_count(), BF_HASH_FUNCS);

        m_alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);

        TIMER_INIT();

        size_t offset = 0;
        m_reccnt = 0;
        auto base = buffer->get_data();
        auto stop = base + buffer->get_record_count();

        TIMER_START();
        std::sort(base, stop, std::less<Wrapped<R>>());
        TIMER_STOP();
        auto sort_time = TIMER_RESULT();

        TIMER_START();
        while (base < stop) {
            if (!base->is_tombstone() && (base + 1 < stop)
                && base->rec == (base + 1)->rec  && (base + 1)->is_tombstone()) {
                base += 2;
                mrun_cancelations++;
                continue;
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
            if (m_bf && base->is_tombstone()) {
                ++m_tombstone_cnt;
                m_bf->insert(base->rec);
            }

            base++;
        }
        TIMER_STOP();
        auto copy_time = TIMER_RESULT();

        TIMER_START();
        if (m_reccnt > 0) {
            build_internal_levels();
        }
        TIMER_STOP();
        auto level_time = TIMER_RESULT();
    }

    MemISAM(MemISAM** runs, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0), m_deleted_cnt(0), m_isam_nodes(nullptr) {
        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(len);

        PriorityQueue<Wrapped<R>> pq(len);

        size_t attemp_reccnt = 0;
        size_t tombstone_count = 0;
        
        for (size_t i = 0; i < len; ++i) {
            if (runs[i]) {
                auto base = runs[i]->get_data();
                cursors.emplace_back(Cursor{base, base + runs[i]->get_record_count(), 0, runs[i]->get_record_count()});
                attemp_reccnt += runs[i]->get_record_count();
                tombstone_count += runs[i]->get_tombstone_count();
                pq.push(cursors[i].ptr, i);
            } else {
                cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
            }
        }

        m_bf = new BloomFilter<R>(BF_FPR, tombstone_count, BF_HASH_FUNCS);

        m_alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);

        size_t offset = 0;
        
        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[now.version];
                auto& cursor2 = cursors[next.version];
                if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[now.version];
                if (!cursor.ptr->is_deleted()) {
                    m_data[m_reccnt++] = *cursor.ptr;
                    if (cursor.ptr->is_tombstone()) {
                        ++m_tombstone_cnt;
                        m_bf->insert(cursor.ptr->rec);
                    }
                }
                pq.pop();
                
                if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        if (m_reccnt > 0) {
            build_internal_levels();
        }
    }

    ~MemISAM() {
        if (m_data) free(m_data);
        if (m_isam_nodes) free(m_isam_nodes);
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
        return (idx < m_reccnt) ? m_data + idx : nullptr;
    }

    size_t get_memory_usage() {
        return m_internal_node_cnt * inmem_isam_node_size + m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        return 0;
    }

private:
    size_t get_lower_bound(const K& key) const {
        const InMemISAMNode* now = m_root;
        while (!is_leaf(reinterpret_cast<const char*>(now))) {
            const InMemISAMNode* next = nullptr;
            for (size_t i = 0; i < inmem_isam_fanout - 1; ++i) {
                if (now->child[i + 1] == nullptr || key <= now->keys[i]) {
                    next = reinterpret_cast<InMemISAMNode*>(now->child[i]);
                    break;
                }
            }

            now = next ? next : reinterpret_cast<const InMemISAMNode*>(now->child[inmem_isam_fanout - 1]);
        }

        const Wrapped<R>* pos = reinterpret_cast<const Wrapped<R>*>(now);
        while (pos < m_data + m_reccnt && pos->rec.key < key) pos++;

        return pos - m_data;
    }

    size_t get_upper_bound(const K& key) const {
        const InMemISAMNode* now = m_root;
        while (!is_leaf(reinterpret_cast<const char*>(now))) {
            const InMemISAMNode* next = nullptr;
            for (size_t i = 0; i < inmem_isam_fanout - 1; ++i) {
                if (now->child[i + 1] == nullptr || key < now->keys[i]) {
                    next = reinterpret_cast<InMemISAMNode*>(now->child[i]);
                    break;
                }
            }

            now = next ? next : reinterpret_cast<const InMemISAMNode*>(now->child[inmem_isam_fanout - 1]);
        }

        const Wrapped<R>* pos = reinterpret_cast<const Wrapped<R>*>(now);
        while (pos < m_data + m_reccnt && pos->rec.key <= key) pos++;

        return pos - m_data;
    }

    void build_internal_levels() {
        size_t n_leaf_nodes = m_reccnt / inmem_isam_leaf_fanout + (m_reccnt % inmem_isam_leaf_fanout != 0);
        size_t level_node_cnt = n_leaf_nodes;
        size_t node_cnt = 0;
        do {
            level_node_cnt = level_node_cnt / inmem_isam_fanout + (level_node_cnt % inmem_isam_fanout != 0);
            node_cnt += level_node_cnt;
        } while (level_node_cnt > 1);

        m_alloc_size = (node_cnt * inmem_isam_node_size) + (CACHELINE_SIZE - (node_cnt * inmem_isam_node_size) % CACHELINE_SIZE);
        assert(m_alloc_size % CACHELINE_SIZE == 0);

        m_isam_nodes = (InMemISAMNode*)std::aligned_alloc(CACHELINE_SIZE, m_alloc_size);
        m_internal_node_cnt = node_cnt;
        memset(m_isam_nodes, 0, node_cnt * inmem_isam_node_size);

        InMemISAMNode* current_node = m_isam_nodes;

        const Wrapped<R>* leaf_base = m_data;
        const Wrapped<R>* leaf_stop = m_data + m_reccnt;
        while (leaf_base < leaf_stop) {
            size_t fanout = 0;
            for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                auto rec_ptr = leaf_base + inmem_isam_leaf_fanout * i;
                if (rec_ptr >= leaf_stop) break;
                const Wrapped<R>* sep_key = std::min(rec_ptr + inmem_isam_leaf_fanout - 1, leaf_stop - 1);
                current_node->keys[i] = sep_key->rec.key;
                current_node->child[i] = (char*)rec_ptr;
                ++fanout;
            }
            current_node++;
            leaf_base += fanout * inmem_isam_leaf_fanout;
        }

        auto level_start = m_isam_nodes;
        auto level_stop = current_node;
        auto current_level_node_cnt = level_stop - level_start;
        while (current_level_node_cnt > 1) {
            auto now = level_start;
            while (now < level_stop) {
                size_t child_cnt = 0;
                for (size_t i = 0; i < inmem_isam_fanout; ++i) {
                    auto node_ptr = now + i;
                    ++child_cnt;
                    if (node_ptr >= level_stop) break;
                    current_node->keys[i] = node_ptr->keys[inmem_isam_fanout - 1];
                    current_node->child[i] = (char*)node_ptr;
                }
                now += child_cnt;
                current_node++;
            }
            level_start = level_stop;
            level_stop = current_node;
            current_level_node_cnt = level_stop - level_start;
        }
        
        assert(current_level_node_cnt == 1);
        m_root = level_start;
    }

    bool is_leaf(const char* ptr) const {
        return ptr >= (const char*)m_data && ptr < (const char*)(m_data + m_reccnt);
    }

    // Members: sorted data, internal ISAM levels, reccnt;
    Wrapped<R>* m_data;
    psudb::BloomFilter<R> *m_bf;
    InMemISAMNode* m_isam_nodes;
    InMemISAMNode* m_root;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_internal_node_cnt;
    size_t m_deleted_cnt;
    size_t m_alloc_size;
};
}
