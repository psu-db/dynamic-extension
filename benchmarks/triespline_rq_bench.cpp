/*
 * benchmarks/triespline_rq_bench.cpp
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#include "include/bench.h"

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: triespline_rq_bench <filename> <record_count> <delete_proportion> <query_file> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t buffer_cap = 12000;
    size_t scale_factor = 6;
    double delete_prop = atof(argv[3]);
    double max_delete_prop = (delete_prop > 0) ? delete_prop : 1;
    std::string query_file = std::string(argv[4]);
    bool use_osm = (argc == 6) ? atoi(argv[5]) : 0;

    double insert_batch = 0.1; 

    init_bench_env(record_count, true, use_osm);

    auto de = ExtendedTSRQ(buffer_cap, scale_factor, max_delete_prop);
    auto queries = read_range_queries<de::ts_range_query_parms<Rec>>(query_file, .0001);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    std::vector<Rec> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup<ExtendedTSRQ, Rec>(datafile, de, warmup_cnt, delete_prop, to_delete);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_tput_bench<ExtendedTSRQ, Rec>(de, datafile, insert_cnt, delete_prop, to_delete);
    query_latency_bench<ExtendedTSRQ, Rec, de::ts_range_query_parms<Rec>>(de, queries, 1);

    auto ts = de.create_static_structure();

    static_latency_bench<de::TrieSpline<Rec>, Rec, de::ts_range_query_parms<Rec>, de::TrieSplineRangeQuery<Rec>>(
        ts, queries
    );
    fprintf(stdout, "\n");

    delete ts;

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
