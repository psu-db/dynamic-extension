/*
 * include/query/knn.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for k-NN queries, designed for use with the VPTree
 * shard. 
 *
 * FIXME: no support for tombstone deletes just yet. This would require a
 * query resumption mechanism, most likely.
 */
#pragma once

#include "framework/QueryRequirements.h"
#include "psu-ds/PriorityQueue.h"

namespace de { namespace knn {

using psudb::PriorityQueue;

template <NDRecordInterface R>
struct Parms {
    R point;
    size_t k;
};

template <NDRecordInterface R>
struct State {
    size_t k;
};

template <NDRecordInterface R>
struct BufferState {
    BufferView<R> *buffer;

    BufferState(BufferView<R> *buffer) 
        : buffer(buffer) {}
};

template <NDRecordInterface R, ShardInterface<R> S>
class Query {
public:
    constexpr static bool EARLY_ABORT=false;
    constexpr static bool SKIP_DELETE_FILTER=true;

    static void *get_query_state(S *shard, void *parms) {
        return nullptr;
    }

    static void* get_buffer_query_state(BufferView<R> *buffer, void *parms) {
        return new BufferState<R>(buffer);
    }

    static void process_query_states(void *query_parms, std::vector<void*> &shard_states, void* buffer_state) {
        return;
    }

    static std::vector<Wrapped<R>> query(S *shard, void *q_state, void *parms) {
        std::vector<Wrapped<R>> results;
        Parms<R> *p = (Parms<R> *) parms;
        Wrapped<R> wrec;
        wrec.rec = p->point;
        wrec.header = 0;

        PriorityQueue<Wrapped<R>, DistCmpMax<Wrapped<R>>> pq(p->k, &wrec);

        shard->search(p->point, p->k, pq);

        while (pq.size() > 0) {
            results.emplace_back(*pq.peek().data);
            pq.pop();
        }

        return results;
    }

    static std::vector<Wrapped<R>> buffer_query(void *state, void *parms) {
        Parms<R> *p = (Parms<R> *) parms;
        BufferState<R> *s = (BufferState<R> *) state;
        Wrapped<R> wrec;
        wrec.rec = p->point;
        wrec.header = 0;

        size_t k = p->k;

        PriorityQueue<Wrapped<R>, DistCmpMax<Wrapped<R>>> pq(k, &wrec);
        for (size_t i=0; i<s->buffer->get_record_count(); i++) {
            // Skip over deleted records (under tagging)
            if (s->buffer->get(i)->is_deleted()) {
                continue;
            }

            if (pq.size() < k) {
                pq.push(s->buffer->get(i));
            } else {
                double head_dist = pq.peek().data->rec.calc_distance(wrec.rec);
                double cur_dist = (s->buffer->get(i))->rec.calc_distance(wrec.rec);

                if (cur_dist < head_dist) {
                    pq.pop();
                    pq.push(s->buffer->get(i));
                }
            }
        }

        std::vector<Wrapped<R>> results;
        while (pq.size() > 0) {
            results.emplace_back(*(pq.peek().data));
            pq.pop();
        }

        return std::move(results);
    }

    static std::vector<R> merge(std::vector<std::vector<Wrapped<R>>> &results, void *parms, std::vector<R> &output) {
        Parms<R> *p = (Parms<R> *) parms;
        R rec = p->point;
        size_t k = p->k;

        PriorityQueue<R, DistCmpMax<R>> pq(k, &rec);
        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                if (pq.size() < k) {
                    pq.push(&results[i][j].rec);
                } else {
                    double head_dist = pq.peek().data->calc_distance(rec);
                    double cur_dist = results[i][j].rec.calc_distance(rec);

                    if (cur_dist < head_dist) {
                        pq.pop();
                        pq.push(&results[i][j].rec);
                    }
                }
            }
        }

        while (pq.size() > 0) {
            output.emplace_back(*pq.peek().data);
            pq.pop();
        }

        return std::move(output);
    }

    static void delete_query_state(void *state) {
        auto s = (State<R> *) state;
        delete s;
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
