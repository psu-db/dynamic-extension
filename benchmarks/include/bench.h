#include "bench_utility.h"

template <typename DE, de::RecordInterface R, bool PROGRESS=true, size_t BATCH=1000>
static bool insert_tput_bench(DE &de_index, std::fstream &file, size_t insert_cnt, 
                              double delete_prop, std::vector<R> &to_delete) {

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
        continue_benchmark = build_insert_vec(file, insert_vec, BATCH, delete_prop, to_delete);
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
                de_index.erase(delete_vec[delete_idx++]);
                applied_deletes++;
            }

            // insert the record;
            de_index.insert(insert_vec[i]);
            applied_inserts++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    if constexpr (PROGRESS) {
        progress_update(1.0, "inserting:");
    }

    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stdout, "\n%ld\n", throughput);
    reset_de_perf_metrics();

    return continue_benchmark;
}


