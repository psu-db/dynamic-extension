/*
 * benchmarks/include/bench.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include "bench_utility.h"

template <typename DE, de::RecordInterface R, bool PROGRESS=true, size_t BATCH=1000>
static bool insert_tput_bench(DE &de_index, std::fstream &file, size_t insert_cnt, 
                              double delete_prop, std::vector<R> &to_delete, bool binary=false) {

    size_t delete_cnt = insert_cnt * delete_prop;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    std::vector<R> insert_vec;
    std::vector<R> delete_vec;
    insert_vec.reserve(BATCH);
    delete_vec.reserve(BATCH*delete_prop);

    size_t delete_idx = 0;

    bool continue_benchmark = true;

    size_t total_time = 0;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_insert_vec(file, insert_vec, BATCH, delete_prop, to_delete, binary);
        if (applied_deletes < delete_cnt) {
            build_delete_vec(to_delete, delete_vec, BATCH*delete_prop);
            delete_idx = 0;
        }

        if (insert_vec.size() == 0) {
            break;
        }

        if constexpr (PROGRESS) {
            progress_update((double) applied_inserts / (double) insert_cnt, "inserting:");
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < delete_cnt && delete_idx < delete_vec.size() && gsl_rng_uniform(g_rng) < delete_prop) {
                if constexpr (std::is_same_v<TreeMap, DE>) {
                    de_index.erase_one(delete_vec[delete_idx++].key);
                } else if constexpr (std::is_same_v<MTree, DE>) {
                    de_index.remove(delete_vec[delete_idx++]);
                } else {
                    de_index.erase(delete_vec[delete_idx++]);
                }
                applied_deletes++;
            }

            // insert the record;
            if constexpr (std::is_same_v<MTree, DE>) {
                de_index.add(insert_vec[i]);
            } else {
                de_index.insert(insert_vec[i]);
            }
            applied_inserts++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    if constexpr (PROGRESS) {
        progress_update(1.0, "inserting:");
    }

    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stdout, "%ld\t", throughput);
    reset_de_perf_metrics();

    return continue_benchmark;
}

template <typename DE, de::RecordInterface R, typename QP, bool PROGRESS=true>
static bool query_latency_bench(DE &de_index, std::vector<QP> queries, size_t trial_cnt=100) {
    char progbuf[25];
    if constexpr (PROGRESS) {
        sprintf(progbuf, "querying:");
    }

    size_t total_time = 0;
    size_t total_results = 0;

    for (size_t i=0; i<trial_cnt; i++) {
        if constexpr (PROGRESS) {
            progress_update((double) (i) / (double) trial_cnt, progbuf);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j=0; j<queries.size(); j++) {
            auto res = de_index.query(&queries[j]);
            total_results += res.size();
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    progress_update(1.0, progbuf);

    size_t query_latency = total_time / (trial_cnt * queries.size());

    fprintf(stdout, "%ld\t", query_latency);
    fflush(stdout);

    return true;
}


template <typename Shard, de::RecordInterface R, typename QP, QueryInterface Q, bool PROGRESS=true>
static bool static_latency_bench(Shard *shard, std::vector<QP> queries, size_t trial_cnt=100) {
    char progbuf[25];
    if constexpr (PROGRESS) {
        sprintf(progbuf, "querying:");
    }

    size_t total_time = 0;
    size_t total_results = 0;

    for (size_t i=0; i<trial_cnt; i++) {
        if constexpr (PROGRESS) {
            progress_update((double) (i) / (double) trial_cnt, progbuf);
        }

        std::vector<void *> states(1);

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j=0; j<queries.size(); j++) {
            states[0] = Q::get_query_state(shard, &queries[j]);
            Q::process_query_states(&queries[j], states, nullptr);
            auto res = Q::query(shard, states[0], &queries[j]);
            total_results += res.size();
            Q::delete_query_state(states[0]);
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    progress_update(1.0, progbuf);

    size_t query_latency = total_time / (trial_cnt * queries.size());

    fprintf(stdout, "%ld\t", query_latency);
    fflush(stdout);

    return true;
}
