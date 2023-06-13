#include "include/bench.h"

static void sample_benchmark(ExtendedWSS &de_wss, size_t k, size_t trial_cnt)
{
    char progbuf[25];
    sprintf(progbuf, "sampling (%ld):", k);

    size_t batch_size = 100;
    size_t batches = trial_cnt / batch_size;
    size_t total_time = 0;

    WRec sample_set[k];

    size_t total_samples = 0;

    de::wss_query_parms<WRec> parms;
    parms.rng = g_rng;
    parms.sample_size = k;

    for (int i=0; i<batches; i++) {
        progress_update((double) (i * batch_size) / (double) trial_cnt, progbuf);
        auto start = std::chrono::high_resolution_clock::now();
        for (int j=0; j < batch_size; j++) {
            auto res = de_wss.query(&parms);
            total_samples += res.size();
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    progress_update(1.0, progbuf);

    size_t throughput = (((double)(total_samples) / (double) total_time) * 1e9);

    fprintf(stdout, "%ld\n", throughput);
    fflush(stdout);
}


int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "Usage: sampling_tput <filename> <record_count> <buffer_cap> <scale_factor> <delete_proportion> <max_delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t buffer_cap = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    double delete_prop = atof(argv[5]);
    double max_delete_prop = atof(argv[6]);
    bool use_osm = (argc == 8) ? atoi(argv[7]) : 0;

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

    insert_tput_bench<ExtendedWSS, WRec>(de_wss, datafile, insert_cnt, delete_prop, to_delete);
    sample_benchmark(de_wss, 1000, 10000);

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
