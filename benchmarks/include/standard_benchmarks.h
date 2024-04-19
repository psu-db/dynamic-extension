/*
 * benchmarks/include/bench.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <fstream>
#include <gsl/gsl_rng.h>

#include "framework/DynamicExtension.h"
#include "framework/interface/Query.h"
#include "psu-util/progress.h"
#include "benchmark_types.h"

static size_t g_deleted_records = 0;
static double delete_proportion = 0.05;

template<typename DE, typename QP>
static void run_queries(DE *extension, std::vector<QP> &queries) {
    size_t total;
    for (size_t i=0; i<queries.size(); i++) {
        auto q = &queries[i];

        auto res = extension->query(q);
        auto r = res.get();
        total += r.size();
    }
}


template<typename S, typename QP, typename Q>
static void run_static_queries(S *shard, std::vector<QP> &queries) {
    size_t total;
    for (size_t i=0; i<queries.size(); i++) {
        auto q = &queries[i];

        auto state = Q::get_query_state(shard, q);
        auto res = Q::query(shard, state, q);

        total += res.size();
    }
}




template<typename DE, de::RecordInterface R>
static void insert_records(DE *extension, size_t start, size_t stop, 
                           std::vector<R> &records, std::vector<size_t> &to_delete, 
                           size_t &delete_idx, bool delete_records, gsl_rng *rng) {

    psudb::progress_update(0, "Insert Progress");
    size_t reccnt = 0;
    for (size_t i=start; i<stop; i++) {
        while (!extension->insert(records[i])) {
            psudb::progress_update((double) i / (double)(stop - start), "Insert Progress");
            usleep(1);
        }

        if (delete_records && gsl_rng_uniform(rng) <= 
            delete_proportion && to_delete[delete_idx] <= i) {

            while (!extension->erase(records[to_delete[delete_idx]])) {
                usleep(1);
            }

            delete_idx++;
            g_deleted_records++;
        }
    }

    psudb::progress_update(1, "Insert Progress");
}

template <typename DE, de::RecordInterface R, bool PROGRESS=true, size_t BATCH=1000>
static bool insert_tput_bench(DE &de_index, std::fstream &file, size_t insert_cnt, 
                              double delete_prop, gsl_rng *rng, std::vector<R> &to_delete, bool binary=false) {

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
            psudb::progress_update((double) applied_inserts / (double) insert_cnt, "inserting:");
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < delete_cnt && delete_idx < delete_vec.size() && gsl_rng_uniform(rng) < delete_prop) {
                if constexpr (std::is_same_v<BenchBTree, DE>) {
                    de_index.erase_one(delete_vec[delete_idx++].key);
                #ifdef _GNU_SOURCE
                } else if constexpr (std::is_same_v<MTree, DE>) {
                    de_index.remove(delete_vec[delete_idx++]);
                #endif
                } else {
                    de_index.erase(delete_vec[delete_idx++]);
                }
                applied_deletes++;
            }

            // insert the record;
            #ifdef _GNU_SOURCE
            if constexpr (std::is_same_v<MTree, DE>) {
                de_index.add(insert_vec[i]);
            } else {
                de_index.insert(insert_vec[i]);
            }
            #else
            de_index.insert(insert_vec[i]);
            #endif

            applied_inserts++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    if constexpr (PROGRESS) {
        psudb::progress_update(1.0, "inserting:");
    }

    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stdout, "%ld\t", throughput);

    return continue_benchmark;
}

template <typename DE, de::RecordInterface R, typename QP, bool PROGRESS=true>
static bool query_latency_bench(DE &de_index, std::vector<QP> queries, size_t trial_cnt=1) {
    char progbuf[25];
    if constexpr (PROGRESS) {
        sprintf(progbuf, "querying:");
    }

    size_t total_time = 0;
    size_t total_results = 0;

    for (size_t i=0; i<trial_cnt; i++) {
        if constexpr (PROGRESS) {
            psudb::progress_update((double) (i) / (double) trial_cnt, progbuf);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j=0; j<queries.size(); j++) {
            auto res = de_index.query(&queries[j]);

            total_results += res.size();
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    psudb::progress_update(1.0, progbuf);

    size_t query_latency = total_time / (trial_cnt * queries.size());

    fprintf(stdout, "%ld\t", query_latency);
    fflush(stdout);

    return true;
}


template <typename Shard, de::RecordInterface R, typename QP, de::QueryInterface<R, Shard> Q, bool PROGRESS=true>
static bool static_latency_bench(Shard *shard, std::vector<QP> queries, size_t trial_cnt=100) {
    char progbuf[25];
    if constexpr (PROGRESS) {
        sprintf(progbuf, "querying:");
    }

    size_t total_time = 0;
    size_t total_results = 0;

    for (size_t i=0; i<trial_cnt; i++) {
        if constexpr (PROGRESS) {
            psudb::progress_update((double) (i) / (double) trial_cnt, progbuf);
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

    psudb::progress_update(1.0, progbuf);

    size_t query_latency = total_time / (trial_cnt * queries.size());

    fprintf(stdout, "%ld\t", query_latency);
    fflush(stdout);

    return true;
}
