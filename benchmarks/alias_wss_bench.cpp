/*
 * benchmarks/alias_wss_bench.cpp
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#include "include/bench.h"

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: sampling_tput <filename> <record_count> <delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t buffer_cap = 12000;
    size_t scale_factor = 6;
    double delete_prop = atof(argv[3]);
    double max_delete_prop = (delete_prop > 0) ? delete_prop : 1;
    bool use_osm = (argc == 5) ? atoi(argv[4]) : 0;

    double insert_batch = 0.1; 

    init_bench_env(record_count, true, use_osm);

    auto de_wss = ExtendedWSS(buffer_cap, scale_factor, max_delete_prop);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    std::vector<WRec> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup<ExtendedWSS, WRec>(datafile, de_wss, warmup_cnt, delete_prop, to_delete);

    size_t insert_cnt = record_count - warmup_cnt;

    std::vector<de::wss_query_parms<WRec>> queries(1);
    queries[0].rng = g_rng;
    queries[0].sample_size = 1000;

    insert_tput_bench<ExtendedWSS, WRec>(de_wss, datafile, insert_cnt, delete_prop, to_delete);
    query_latency_bench<ExtendedWSS, WRec, de::wss_query_parms<WRec>>(de_wss, queries, 1000);
    fprintf(stdout, "\n");

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
