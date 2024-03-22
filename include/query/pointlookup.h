/*
 * include/query/pointlookup.h
 *
 * Copyright (C) 2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for point lookup operations.
 *
 * TODO: Currently, this only supports point lookups for unique keys (which
 * is the case for the trie that we're building this to use). It would be
 * pretty straightforward to extend it to return *all* records that match
 * the search_key (including tombstone cancellation--it's invertible) to
 * support non-unique indexes, or at least those implementing
 * lower_bound().
 */
#pragma once

#include "framework/QueryRequirements.h"

namespace de { namespace pl {

template <RecordInterface R>
struct Parms {
    decltype(R::key) search_key;
};

template <RecordInterface R>
struct State {
};

template <RecordInterface R>
struct BufferState {
    BufferView<R> *buffer;

    BufferState(BufferView<R> *buffer) 
        : buffer(buffer) {}
};

template <KVPInterface R, ShardInterface<R> S>
class Query {
public:
    constexpr static bool EARLY_ABORT=true;
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
        auto p = (Parms<R> *) parms;
        auto s = (State<R> *) q_state;

        std::vector<Wrapped<R>> result;

        auto r = shard->point_lookup({p->search_key, 0});

        if (r) {
            result.push_back(*r);
        }

        return result;
    }

    static std::vector<Wrapped<R>> buffer_query(void *state, void *parms) {
        auto p = (Parms<R> *) parms;
        auto s = (BufferState<R> *) state;

        std::vector<Wrapped<R>> records;
        for (size_t i=0; i<s->buffer->get_record_count(); i++) {
            auto rec = s->buffer->get(i);

            if (rec->rec.key == p->search_key) {
                records.push_back(*rec);
                return records;
            }
        }

        return records;
    }

    static std::vector<R> merge(std::vector<std::vector<Wrapped<R>>> &results, void *parms) {
        std::vector<R> output;
        for (auto r : results) {
            if (r.size() > 0) {
                if (r[0].is_deleted() || r[0].is_tombstone()) {
                    return output;
                }

                output.append(r[0].rec);
                return output;
            }
        }
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
