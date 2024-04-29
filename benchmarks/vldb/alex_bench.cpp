#define ENABLE_TIMER

#include "alex.h"

#include "file_util.h"
#include "psu-util/progress.h"
#include "psu-util/timer.h"

typedef uint64_t key_type;
typedef uint64_t value_type;

typedef alex::Alex<key_type, value_type> Alex;

struct record {
    key_type key;
    value_type value;
};

struct query {
    key_type lower_bound;
    key_type upper_bound;
};

void usage(char *progname) {
    fprintf(stderr, "%s reccnt datafile queryfile\n", progname);
}

static size_t g_deleted_records = 0;
static double delete_proportion = 0.05;

static void insert_records(Alex *structure, size_t start, size_t stop, 
                           std::vector<record> &records, std::vector<size_t> &to_delete, 
                           size_t &delete_idx, bool delete_records, gsl_rng *rng) {

    psudb::progress_update(0, "Insert Progress");
    size_t reccnt = 0;
    for (size_t i=start; i<stop; i++) {
        structure->insert(records[i].key, records[i].value);

        if (delete_records && gsl_rng_uniform(rng) <= 
            delete_proportion && to_delete[delete_idx] <= i) {

            structure->erase_one(records[i].key);
            delete_idx++;
            g_deleted_records++;
        }
    }

    psudb::progress_update(1, "Insert Progress");
}

size_t g_global_cnt = 0;

static void run_queries(Alex *alex, std::vector<query> &queries) {
    for (size_t i=0; i<queries.size(); i++) {
        size_t cnt=0; 
        auto ptr = alex->find(queries[i].lower_bound);
        while (ptr != alex->end() && ptr.key() <= queries[i].upper_bound) {
            cnt++;
            ptr++;
        }

        g_global_cnt += cnt;
    }
}

Alex *warmup_alex(std::vector<record> records, size_t cnt) {
    if (cnt >= records.size()) {
        fprintf(stderr, "[E] Requesting warmup with more records than are available.\n");
        exit(EXIT_FAILURE);
    }

    auto alex = new Alex();
    std::pair<key_type, value_type> *insert_vec = new std::pair<key_type, value_type>[cnt];

    for (size_t i=0; i<cnt; i++) {
        insert_vec[i] = {records[i].key, records[i].value};
    }

    std::sort(insert_vec, insert_vec + cnt);
    alex->bulk_load(insert_vec, cnt);
    delete[] insert_vec;

    return alex;
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = std::string(argv[2]);
    std::string q_fname = std::string(argv[3]);

    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);


    auto data = read_sosd_file<record>(d_fname, n);
    std::vector<size_t> to_delete(n * delete_proportion);
    size_t j=0;
    for (size_t i=0; i<data.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= delete_proportion) {
            to_delete[j++] = i;
        } 
    }

    auto queries = read_range_queries<query>(q_fname, .001);


    size_t warmup = .1 * n;
    size_t delete_idx = 0;

    auto alex = warmup_alex(data, warmup);

    TIMER_INIT();

    TIMER_START();
    insert_records(alex, warmup, data.size(), data, to_delete, delete_idx, true, rng);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    TIMER_START();
    run_queries(alex, queries);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    auto ext_size = alex->model_size() + alex->data_size() - (alex->size() * sizeof(record));

    fprintf(stdout, "%ld\t%ld\t%lld\t%ld\n", insert_throughput, query_latency, ext_size, g_global_cnt);
    fflush(stdout);

    gsl_rng_free(rng);
    fflush(stderr);

    delete alex;

    exit(EXIT_SUCCESS);
}
