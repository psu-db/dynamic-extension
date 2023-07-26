/*
 * include/shard/MemISAM.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
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

#include "framework/MutableBuffer.h"
#include "util/bf_config.h"
#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "util/timer.h"

namespace de {

thread_local size_t mrun_cancelations = 0;

template <RecordInterface R>
struct irs_query_parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
    size_t sample_size;
    gsl_rng *rng;
};

template <RecordInterface R, bool Rejection>
class IRSQuery;

template <RecordInterface R>
struct IRSState {
    size_t lower_bound;
    size_t upper_bound;
    size_t sample_size;
    size_t total_weight;
};

template <RecordInterface R>
struct IRSBufferState {
    size_t cutoff;
    std::vector<Wrapped<R>> records;
    size_t sample_size;
};

template <RecordInterface R>
struct ISAMRangeQueryParms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
};

template <RecordInterface R>
class ISAMRangeQuery;

template <RecordInterface R>
struct ISAMRangeQueryState {
    size_t start_idx;
    size_t stop_idx;
};

template <RecordInterface R>
struct RangeQueryBufferState {
    size_t cutoff;
};

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
    BloomFilter<R> *m_bf;
    InMemISAMNode* m_isam_nodes;
    InMemISAMNode* m_root;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_internal_node_cnt;
    size_t m_deleted_cnt;
    size_t m_alloc_size;
};

template <RecordInterface R, bool Rejection=true>
class IRSQuery {
public:

    static void *get_query_state(MemISAM<R> *isam, void *parms) {
        auto res = new IRSState<R>();
        decltype(R::key) lower_key = ((irs_query_parms<R> *) parms)->lower_bound;
        decltype(R::key) upper_key = ((irs_query_parms<R> *) parms)->upper_bound;

        res->lower_bound = isam->get_lower_bound(lower_key);
        res->upper_bound = isam->get_upper_bound(upper_key);

        if (res->lower_bound == isam->get_record_count()) {
            res->total_weight = 0;
        } else {
            res->total_weight = res->upper_bound - res->lower_bound;
        }

        res->sample_size = 0;
        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        auto res = new IRSBufferState<R>();

        res->cutoff = buffer->get_record_count();
        res->sample_size = 0;

        if constexpr (Rejection) {
            return res;
        }

        auto lower_key = ((irs_query_parms<R> *) parms)->lower_bound;
        auto upper_key = ((irs_query_parms<R> *) parms)->upper_bound;

        for (size_t i=0; i<res->cutoff; i++) {
            if (((buffer->get_data() + i)->rec.key >= lower_key) && ((buffer->get_data() + i)->rec.key <= upper_key)) { 
                res->records.emplace_back(*(buffer->get_data() + i));
            }
        }

        return res;
    }

    static void process_query_states(void *query_parms, std::vector<void*> shard_states, void *buff_state) {
        auto p = (irs_query_parms<R> *) query_parms;
        auto bs = (buff_state) ? (IRSBufferState<R> *) buff_state : nullptr;

        std::vector<size_t> shard_sample_sizes(shard_states.size()+1, 0);
        size_t buffer_sz = 0;

        std::vector<size_t> weights;
        if constexpr (Rejection) {
            weights.push_back((bs) ? bs->cutoff : 0);
        } else {
            weights.push_back((bs) ? bs->records.size() : 0);
        }

        size_t total_weight = 0;
        for (auto &s : shard_states) {
            auto state = (IRSState<R> *) s;
            total_weight += state->total_weight;
            weights.push_back(state->total_weight);
        }

        // if no valid records fall within the query range, just
        // set all of the sample sizes to 0 and bail out.
        if (total_weight == 0) {
            for (size_t i=0; i<shard_states.size(); i++) {
                auto state = (IRSState<R> *) shard_states[i];
                state->sample_size = 0;
            }

            return;
        }

        std::vector<double> normalized_weights;
        for (auto w : weights) {
            normalized_weights.push_back((double) w / (double) total_weight);
        }

        auto shard_alias = Alias(normalized_weights);
        for (size_t i=0; i<p->sample_size; i++) {
            auto idx = shard_alias.get(p->rng);            
            if (idx == 0) {
                buffer_sz++;
            } else {
                shard_sample_sizes[idx - 1]++;
            }
        }

        if (bs) {
            bs->sample_size = buffer_sz;
        }
        for (size_t i=0; i<shard_states.size(); i++) {
            auto state = (IRSState<R> *) shard_states[i];
            state->sample_size = shard_sample_sizes[i+1];
        }
    }

    static std::vector<Wrapped<R>> query(MemISAM<R> *isam, void *q_state, void *parms) { 
        auto lower_key = ((irs_query_parms<R> *) parms)->lower_bound;
        auto upper_key = ((irs_query_parms<R> *) parms)->upper_bound;
        auto rng = ((irs_query_parms<R> *) parms)->rng;

        auto state = (IRSState<R> *) q_state;
        auto sample_sz = state->sample_size;

        std::vector<Wrapped<R>> result_set;

        if (sample_sz == 0 || state->lower_bound == isam->get_record_count()) {
            return result_set;
        }

        size_t attempts = 0;
        size_t range_length = state->upper_bound - state->lower_bound;
        do {
            attempts++;
            size_t idx = (range_length > 0) ? gsl_rng_uniform_int(rng, range_length) : 0;
            result_set.emplace_back(*isam->get_record_at(state->lower_bound + idx));
        } while (attempts < sample_sz);

        return result_set;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto st = (IRSBufferState<R> *) state;
        auto p = (irs_query_parms<R> *) parms;

        std::vector<Wrapped<R>> result;
        result.reserve(st->sample_size);

        if constexpr (Rejection) {
            for (size_t i=0; i<st->sample_size; i++) {
                auto idx = gsl_rng_uniform_int(p->rng, st->cutoff);
                auto rec = buffer->get_data() + idx;

                if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                    result.emplace_back(*rec);
                }
            }

            return result;
        }

        for (size_t i=0; i<st->sample_size; i++) {
            auto idx = gsl_rng_uniform_int(p->rng, st->records.size());
            result.emplace_back(st->records[idx]);
        }

        return result;
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results, void *parms) {
        std::vector<R> output;

        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                output.emplace_back(results[i][j]);
            }
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (IRSState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (IRSBufferState<R> *) state;
        delete s;
    }
};


template <RecordInterface R>
class ISAMRangeQuery {
public:
    static void *get_query_state(MemISAM<R> *ts, void *parms) {
        auto res = new ISAMRangeQueryState<R>();
        auto p = (ISAMRangeQueryParms<R> *) parms;

        res->start_idx = ts->get_lower_bound(p->lower_bound);
        res->stop_idx = ts->get_record_count();

        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        auto res = new RangeQueryBufferState<R>();
        res->cutoff = buffer->get_record_count();

        return res;
    }

    static void process_query_states(void *query_parms, std::vector<void*> shard_states, void *buff_state) {
        return;
    }

    static std::vector<Wrapped<R>> query(MemISAM<R> *ts, void *q_state, void *parms) {
        std::vector<Wrapped<R>> records;
        auto p = (ISAMRangeQueryParms<R> *) parms;
        auto s = (ISAMRangeQueryState<R> *) q_state;

        // if the returned index is one past the end of the
        // records for the PGM, then there are not records
        // in the index falling into the specified range.
        if (s->start_idx == ts->get_record_count()) {
            return records;
        }

        auto ptr = ts->get_record_at(s->start_idx);
        
        // roll the pointer forward to the first record that is
        // greater than or equal to the lower bound.
        while(ptr->rec.key < p->lower_bound) {
            ptr++;
        }

        while (ptr->rec.key <= p->upper_bound && ptr < ts->m_data + s->stop_idx) {
            records.emplace_back(*ptr);
            ptr++;
        }

        return records;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto p = (ISAMRangeQueryParms<R> *) parms;
        auto s = (RangeQueryBufferState<R> *) state;

        std::vector<Wrapped<R>> records;
        for (size_t i=0; i<s->cutoff; i++) {
            auto rec = buffer->get_data() + i;
            if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                records.emplace_back(*rec);
            }
        }

        return records;
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results, void *parms) {
        size_t total = 0;
        for (size_t i=0; i<results.size(); i++) {
            total += results[i].size();
        }

        if (total == 0) {
            return std::vector<R>();
        }

        std::vector<R> output;
        output.reserve(total);

        for (size_t i=0; i<results.size(); i++) {
            std::move(results[i].begin(), results[i].end(), std::back_inserter(output));
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (ISAMRangeQueryState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (RangeQueryBufferState<R> *) state;
        delete s;
    }
};



}
