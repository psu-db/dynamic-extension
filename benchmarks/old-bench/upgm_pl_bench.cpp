#include "pgm/pgm_index_dynamic.hpp"
#include "include/standalone_utility.h"

typedef uint64_t key_type;
typedef uint64_t value_type;

typedef pgm::DynamicPGMIndex<key_type, value_type, pgm::PGMIndex<key_type, 64>> PGM;

struct record {
    key_type key;
    value_type value;
};

struct query {
    key_type lower_bound;
    key_type upper_bound;
};

template <typename R>
static bool build_insert_vec(std::fstream &file, std::vector<R> &vec, size_t n, 
                             double delete_prop, std::vector<R> &to_delete, bool binary=false) {
    vec.clear();
    for (size_t i=0; i<n; i++) {
        R rec;
        if (!next_record(file, rec, binary)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.emplace_back(rec);

        if (gsl_rng_uniform(g_rng) < delete_prop + (delete_prop * .1)) {
            to_delete.emplace_back(rec);
        }
    }

    return true;
}


static bool warmup(std::fstream &file, PGM &pgm, size_t count, 
                   double delete_prop, std::vector<record> to_delete, bool progress=true, bool binary=false) {
    size_t batch = std::min(.1 * count, 25000.0);

    std::vector<record> insert_vec;
    std::vector<record> delete_vec;
    insert_vec.reserve(batch);
    delete_vec.reserve(batch*delete_prop);

    size_t inserted = 0;
    size_t delete_idx = 0;

    double last_percent = 0;
    while (inserted < count) {
        // Build vector of records to insert and potentially delete
        auto continue_warmup = build_insert_vec<record>(file, insert_vec, batch, delete_prop, to_delete, binary);
        if (inserted > batch) {
            build_delete_vec(to_delete, delete_vec, batch*delete_prop);
            delete_idx = 0;
        }

        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (delete_idx < delete_vec.size() && gsl_rng_uniform(g_rng) < delete_prop) {
                pgm.erase(delete_vec[delete_idx++].key);
            }

            pgm.insert_or_assign(insert_vec[i].key, insert_vec[i].value);
            inserted++;
            progress_update((double) inserted / (double) count, "warming up:");
        }
    }

    return true;
}


static void pgm_rq_insert(PGM &pgm, std::fstream &file, size_t insert_cnt, double delete_prop, std::vector<record> &to_delete, bool binary=false) {
    size_t delete_cnt = insert_cnt * delete_prop;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    size_t BATCH=1000;

    std::vector<record> insert_vec;
    std::vector<record> delete_vec;
    insert_vec.reserve(BATCH);
    delete_vec.reserve(BATCH*delete_prop);

    size_t delete_idx = 0;

    bool continue_benchmark = true;

    size_t total_time = 0;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_insert_vec(file, insert_vec, BATCH, delete_prop, to_delete, binary);
        progress_update((double) applied_inserts / (double) insert_cnt, "inserting:");
        if (applied_deletes < delete_cnt) {
            build_delete_vec(to_delete, delete_vec, BATCH*delete_prop);
            delete_idx = 0;
        }

        if (insert_vec.size() == 0) {
            break;
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < delete_cnt && delete_idx < delete_vec.size() && gsl_rng_uniform(g_rng) < delete_prop) {
                pgm.erase(delete_vec[delete_idx++].key);
                applied_deletes++;
            }

            // insert the record;
            pgm.insert_or_assign(insert_vec[i].key, insert_vec[i].value);
            applied_inserts++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    progress_update(1.0, "inserting:");

    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stdout, "%ld\t", throughput);
}



static void pgm_pl_bench(PGM &pgm, std::vector<query> queries, size_t trial_cnt=1) 
{
    char progbuf[25];
    sprintf(progbuf, "sampling:");

    size_t batch_size = 100;
    size_t batches = trial_cnt / batch_size;
    size_t total_time = 0;

    std::vector<record> result_set;

    for (int i=0; i<trial_cnt; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j=0; j<queries.size(); j++) {
            auto ptr = pgm.find(queries[j].lower_bound);
            if (ptr != pgm.end() && ptr->first == queries[j].lower_bound) {
                result_set.push_back({ptr->first, ptr->second});
            }
            result_set.clear();
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    size_t latency = total_time / (trial_cnt * queries.size());

    fprintf(stdout, "%ld\t", latency);
}

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: upgm_pl_bench <filename> <record_count> <delete_proportion> <query_file>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double delete_prop = atof(argv[3]);
    std::string qfilename = std::string(argv[4]);

    double insert_batch = 0.1; 

    init_bench_env(record_count, true);
    auto queries = read_range_queries<query>(qfilename, .0001);

    std::vector<std::pair<key_type, value_type>> data;
    PGM pgm(data.begin(), data.end());

    std::fstream datafile;
    datafile.open(filename, std::ios::in | std::ios::binary);

    std::vector<record> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup(datafile, pgm, warmup_cnt, delete_prop, to_delete, true, true);

    size_t insert_cnt = record_count - warmup_cnt;

    pgm_rq_insert(pgm, datafile, insert_cnt, delete_prop, to_delete, true);
    size_t memory_usage = pgm.size_in_bytes();
    fprintf(stdout, "%ld\t", memory_usage);

    pgm_pl_bench(pgm, queries);
    fprintf(stdout, "\n");

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
