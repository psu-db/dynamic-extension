#include "bench.h"

size_t g_insert_batch_size = 1000;

static bool insert_benchmark(ExtendedWSS *tree, std::fstream *file, 
                      size_t insert_cnt, double delete_prop) {

    size_t delete_cnt = insert_cnt * delete_prop;
    size_t delete_batch_size = g_insert_batch_size * delete_prop * 15;
    size_t delete_idx = delete_batch_size;

    std::vector<WRec> delbuf;

    std::set<WRec> deleted;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    std::vector<WRec> insert_vec;
    insert_vec.reserve(g_insert_batch_size);
    bool continue_benchmark = true;

    size_t total_time = 0;

    de::wss_query_parms<WRec> parms;
    parms.rng = g_rng;
    parms.sample_size = delete_batch_size;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_insert_vec(file, insert_vec, g_insert_batch_size);

        if (insert_vec.size() == 0) {
            break;
        }

        // if we've fully processed the delete vector, sample a new
        // set of records to delete.
        if (delete_idx >= delbuf.size()) {
            delbuf = tree->query(&parms);
            deleted.clear();
            delete_idx = 0;
        }

        progress_update((double) applied_inserts / (double) insert_cnt, "inserting:");
        size_t local_inserted = 0;
        size_t local_deleted = 0;
        
        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < delete_cnt && delete_idx < delete_batch_size && gsl_rng_uniform(g_rng) < delete_prop) {
                auto rec = delbuf[delete_idx];
                delete_idx++;

                if (deleted.find(rec) == deleted.end()) {
                    tree->erase(rec);
                    deleted.insert(rec);
                    local_deleted++;
                } 
            }

            // insert the record;
            tree->insert(insert_vec[i]);
            local_inserted++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        applied_deletes += local_deleted;
        applied_inserts += local_inserted;
        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    progress_update(1.0, "inserting:");
    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stdout, "\n%ld\n", throughput);
    reset_de_perf_metrics();

    return continue_benchmark;
}


static void sample_benchmark(ExtendedWSS *tree, size_t k, size_t trial_cnt)
{
    char progbuf[25];
    sprintf(progbuf, "sampling (%ld):", k);

    size_t batch_size = 100;
    size_t batches = trial_cnt / batch_size;
    size_t total_time = 0;

    WRec sample_set[k];

    de::wss_query_parms<WRec> parms;
    parms.rng = g_rng;
    parms.sample_size = k;

    for (int i=0; i<batches; i++) {
        progress_update((double) (i * batch_size) / (double) trial_cnt, progbuf);
        auto start = std::chrono::high_resolution_clock::now();
        for (int j=0; j < batch_size; j++) {
            auto res = tree->query(&parms);
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    progress_update(1.0, progbuf);

    size_t throughput = (((double)(trial_cnt * k) / (double) total_time) * 1e9);

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

    auto sampling_lsm = ExtendedWSS(buffer_cap, scale_factor, max_delete_prop);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, warmup_cnt, delete_prop);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_benchmark(&sampling_lsm, &datafile, insert_cnt, delete_prop);
//    sample_benchmark(&sampling_lsm, 1000, 10000);

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
