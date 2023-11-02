/*
 * include/query/irs.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include "framework/QueryRequirements.h"

namespace de { namespace irs {

template <RecordInterface R>
struct Parms {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
    size_t sample_size;
    gsl_rng *rng;
};


template <RecordInterface R>
struct State {
    size_t lower_bound;
    size_t upper_bound;
    size_t sample_size;
    size_t total_weight;
};

template <RecordInterface R>
struct BufferState {
    size_t cutoff;
    std::vector<Wrapped<R>> records;
    size_t sample_size;
};

template <ShardInterface S, RecordInterface R, bool Rejection=true>
class Query {
public:
    constexpr static bool EARLY_ABORT=false;
    constexpr static bool SKIP_DELETE_FILTER=false;

    static void *get_query_state(S *shard, void *parms) {
        auto res = new State<R>();
        decltype(R::key) lower_key = ((PARMS<R> *) parms)->lower_bound;
        decltype(R::key) upper_key = (PARMS<R> *) parms)->upper_bound;

        res->lower_bound = shard->get_lower_bound(lower_key);
        res->upper_bound = shard->get_upper_bound(upper_key);

        if (res->lower_bound == shard->get_record_count()) {
            res->total_weight = 0;
        } else {
            res->total_weight = res->upper_bound - res->lower_bound;
        }

        res->sample_size = 0;
        return res;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        auto res = new BufferState<R>();

        res->cutoff = buffer->get_record_count();
        res->sample_size = 0;

        if constexpr (Rejection) {
            return res;
        }

        auto lower_key = ((Parms<R> *) parms)->lower_bound;
        auto upper_key = ((Parms<R> *) parms)->upper_bound;

        for (size_t i=0; i<res->cutoff; i++) {
            if (((buffer->get_data() + i)->rec.key >= lower_key) && ((buffer->get_data() + i)->rec.key <= upper_key)) { 
                res->records.emplace_back(*(buffer->get_data() + i));
            }
        }

        return res;
    }

    static void process_query_states(void *query_parms, std::vector<void*> &shard_states, void *buff_state) {
        auto p = (Parms<R> *) query_parms;
        auto bs = (buff_state) ? (BufferState<R> *) buff_state : nullptr;

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
            auto state = (State<R> *) s;
            total_weight += state->total_weight;
            weights.push_back(state->total_weight);
        }

        // if no valid records fall within the query range, just
        // set all of the sample sizes to 0 and bail out.
        if (total_weight == 0) {
            for (size_t i=0; i<shard_states.size(); i++) {
                auto state = (State<R> *) shard_states[i];
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
            auto state = (State<R> *) shard_states[i];
            state->sample_size = shard_sample_sizes[i+1];
        }
    }

    static std::vector<Wrapped<R>> query(S *shard, void *q_state, void *parms) { 
        auto lower_key = ((Parms<R> *) parms)->lower_bound;
        auto upper_key = ((Parms<R> *) parms)->upper_bound;
        auto rng = ((Parms<R> *) parms)->rng;

        auto state = (State<R> *) q_state;
        auto sample_sz = state->sample_size;

        std::vector<Wrapped<R>> result_set;

        if (sample_sz == 0 || state->lower_bound == shard->get_record_count()) {
            return result_set;
        }

        size_t attempts = 0;
        size_t range_length = state->upper_bound - state->lower_bound;
        do {
            attempts++;
            size_t idx = (range_length > 0) ? gsl_rng_uniform_int(rng, range_length) : 0;
            result_set.emplace_back(*shard->get_record_at(state->lower_bound + idx));
        } while (attempts < sample_sz);

        return result_set;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        auto st = (BufferState<R> *) state;
        auto p = (Parms<R> *) parms;

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

    static std::vector<R> merge(std::vector<std::vector<Wrapped<R>>> &results, void *parms) {
        std::vector<R> output;

        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                output.emplace_back(results[i][j].rec);
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
