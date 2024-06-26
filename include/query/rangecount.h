/*
 * include/query/rangecount.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for single dimensional range count queries. This query 
 * requires that the shard support get_lower_bound(key) and 
 * get_record_at(index).
 */
#pragma once

#include "framework/QueryRequirements.h"

namespace de { namespace rc {

template <RecordInterface R>
struct Parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
};

template <RecordInterface R>
struct State {
    size_t start_idx;
    size_t stop_idx;
};

template <RecordInterface R>
struct BufferState {
    BufferView<R> *buffer;

    BufferState(BufferView<R> *buffer) 
        : buffer(buffer) {}
};

template <KVPInterface R, ShardInterface<R> S, bool FORCE_SCAN=false>
class Query {
public:
    constexpr static bool EARLY_ABORT=false;
    constexpr static bool SKIP_DELETE_FILTER=true;

    static void *get_query_state(S *shard, void *parms) {
        return nullptr;
    }

    static void* get_buffer_query_state(BufferView<R> *buffer, void *parms) {
        auto res = new BufferState<R>(buffer);

        return res;
    }

    static void process_query_states(void *query_parms, std::vector<void*> &shard_states, void* buffer_state) {
        return;
    }

    static std::vector<Wrapped<R>> query(S *shard, void *q_state, void *parms) {
        std::vector<Wrapped<R>> records;
        auto p = (Parms<R> *) parms;
        auto s = (State<R> *) q_state;

        size_t reccnt = 0;
        size_t tscnt = 0;

        Wrapped<R> res;
        res.rec.key= 0; // records
        res.rec.value = 0; // tombstones
        records.emplace_back(res);


        auto start_idx = shard->get_lower_bound(p->lower_bound);
        auto stop_idx = shard->get_lower_bound(p->upper_bound);

        /* 
         * if the returned index is one past the end of the
         * records for the PGM, then there are not records
         * in the index falling into the specified range.
         */
        if (start_idx == shard->get_record_count()) {
            return records;
        }

        
        /*
         * roll the pointer forward to the first record that is
         * greater than or equal to the lower bound.
         */
        auto recs = shard->get_data();
        while(start_idx < stop_idx && recs[start_idx].rec.key < p->lower_bound) {
            start_idx++;
        }

        while (stop_idx < shard->get_record_count() && recs[stop_idx].rec.key <= p->upper_bound) {
            stop_idx++;
        }
        size_t idx = start_idx;
        size_t ts_cnt = 0;

        while (idx < stop_idx) {
            ts_cnt += recs[idx].is_tombstone() * 2 + recs[idx].is_deleted();
            idx++;
        }

        records[0].rec.key = idx - start_idx;
        records[0].rec.value = ts_cnt;

        return records;
    }

    static std::vector<Wrapped<R>> buffer_query(void *state, void *parms) {
        auto p = (Parms<R> *) parms;
        auto s = (BufferState<R> *) state;

        std::vector<Wrapped<R>> records;

        Wrapped<R> res;
        res.rec.key= 0; // records
        res.rec.value = 0; // tombstones
        records.emplace_back(res);

        size_t stop_idx;
        if constexpr (FORCE_SCAN) {
            stop_idx = s->buffer->get_capacity() / 2;
        } else {
            stop_idx = s->buffer->get_record_count();
        }

        for (size_t i=0; i<s->buffer->get_record_count(); i++) {
            auto rec = s->buffer->get(i);

            if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound
                    && !rec->is_deleted()) {
                if (rec->is_tombstone()) {
                    records[0].rec.value++;
                } else {
                    records[0].rec.key++;
                }
            }
        }

        return records;
    }

    static std::vector<R> merge(std::vector<std::vector<Wrapped<R>>> &results, void *parms, std::vector<R> &output) {
        R res;
        res.key = 0;
        res.value = 0;
        output.emplace_back(res);

        for (size_t i=0; i<results.size(); i++) {
            output[0].key += results[i][0].rec.key; // records
            output[0].value += results[i][0].rec.value; // tombstones
        }

        output[0].key -= output[0].value;
        return output;
    }

    static void delete_query_state(void *state) {
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (BufferState<R> *) state;
        delete s;
    }

    static bool repeat(void *parms, std::vector<R> &results, std::vector<void*> states, void* buffer_state) {
        return false;
    }
};

}}
