/*
 * include/query/rangequery.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for single dimensional range queries. This query requires 
 * that the shard support get_lower_bound(key) and get_record_at(index).
 */
#pragma once

#include "framework/QueryRequirements.h"
#include "psu-ds/PriorityQueue.h"
#include "util/Cursor.h"

namespace de { namespace rq {

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

template <ShardInterface S, RecordInterface R>
class Query {
public:
    constexpr static bool EARLY_ABORT=false;
    constexpr static bool SKIP_DELETE_FILTER=true;

    static void *get_query_state(S *shard, void *parms) {
        auto res = new State<R>();
        auto p = (Parms<R> *) parms;

        res->start_idx = shard->get_lower_bound(p->lower_bound);
        res->stop_idx = shard->get_record_count();

        return res;
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

        /* 
         * if the returned index is one past the end of the
         * records for the PGM, then there are not records
         * in the index falling into the specified range.
         */
        if (s->start_idx == shard->get_record_count()) {
            return records;
        }

        auto ptr = shard->get_record_at(s->start_idx);
        
        /*
         * roll the pointer forward to the first record that is
         * greater than or equal to the lower bound.
         */
        while(ptr < shard->get_data() + s->stop_idx && ptr->rec.key < p->lower_bound) {
            ptr++;
        }

        while (ptr < shard->get_data() + s->stop_idx && ptr->rec.key <= p->upper_bound) {
            records.emplace_back(*ptr);
            ptr++;
        }

        return records;
    }

    static std::vector<Wrapped<R>> buffer_query(void *state, void *parms) {
        auto p = (Parms<R> *) parms;
        auto s = (BufferState<R> *) state;

        std::vector<Wrapped<R>> records;
        for (size_t i=0; i<s->buffer->get_record_count(); i++) {
            auto rec = s->buffer->get(i);
            if (rec->rec.key >= p->lower_bound && rec->rec.key <= p->upper_bound) {
                records.emplace_back(*rec);
            }
        }

        return records;
    }

    static std::vector<R> merge(std::vector<std::vector<Wrapped<R>>> &results, void *parms) {
        std::vector<Cursor<Wrapped<R>>> cursors;
        cursors.reserve(results.size());

        psudb::PriorityQueue<Wrapped<R>> pq(results.size());
        size_t total = 0;
		size_t tmp_n = results.size();
        

        for (size_t i = 0; i < tmp_n; ++i)
			if (results[i].size() > 0){
	            auto base = results[i].data();
		        cursors.emplace_back(Cursor{base, base + results[i].size(), 0, results[i].size()});
				assert(i == cursors.size() - 1);
			    total += results[i].size();
				pq.push(cursors[i].ptr, tmp_n - i - 1);
			} else {
				cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
			}

        if (total == 0) {
            return std::vector<R>();
        }

        std::vector<R> output;
        output.reserve(total);

        while (pq.size()) {
            auto now = pq.peek();
            auto next = pq.size() > 1 ? pq.peek(1) : psudb::queue_record<Wrapped<R>>{nullptr, 0};
            if (!now.data->is_tombstone() && next.data != nullptr &&
                now.data->rec == next.data->rec && next.data->is_tombstone()) {
                
                pq.pop(); pq.pop();
                auto& cursor1 = cursors[tmp_n - now.version - 1];
                auto& cursor2 = cursors[tmp_n - next.version - 1];
                if (advance_cursor<Wrapped<R>>(cursor1)) pq.push(cursor1.ptr, now.version);
                if (advance_cursor<Wrapped<R>>(cursor2)) pq.push(cursor2.ptr, next.version);
            } else {
                auto& cursor = cursors[tmp_n - now.version - 1];
                if (!now.data->is_tombstone()) output.push_back(cursor.ptr->rec);

                pq.pop();
                
                if (advance_cursor<Wrapped<R>>(cursor)) pq.push(cursor.ptr, now.version);
            }
        }

        return output;
    }

    static void delete_query_state(void *state) {
        auto s = (State<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (BufferState<R> *) state;
        delete s;
    }
};

}}
