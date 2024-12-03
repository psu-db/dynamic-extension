/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "query/irs.h"
#include "shard/ISAMTree.h"
#include "benchmark_types.h"
#include "file_util.h"
#include <mutex>

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef btree_record<int64_t, int64_t> Rec;

typedef de::ISAMTree<Rec> Shard;
typedef de::irs::Query<Shard> Q;
typedef Q::Parameters QP;

std::atomic<bool> inserts_done = false;

std::mutex g_btree_lock;

void query_thread(BenchBTree *tree, std::vector<QP> *queries) {
    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);
    size_t total = 0;

    while (!inserts_done.load()) {
        auto q_idx = gsl_rng_uniform_int(rng, queries->size());

        auto q = (*queries)[q_idx];

        std::vector<int64_t> result;
        g_btree_lock.lock();
        tree->range_sample(q.lower_bound, q.upper_bound, 1000, result, rng);
        g_btree_lock.unlock();

        total += result.size();
        usleep(1);
    }

    fprintf(stderr, "%ld\n", total);

    gsl_rng_free(rng);
}

void insert_thread(BenchBTree *tree, size_t start, std::vector<Rec> *records) {
    for (size_t i=start; i<records->size(); i++) {
        btree_record<int64_t, int64_t> r;
        r.key = (*records)[i].key;
        r.value = i;

        g_btree_lock.lock();
        tree->insert(r);
        g_btree_lock.unlock();

        if (i % 100000 == 0) {
            fprintf(stderr, "Inserted %ld records\n", i);
        }
    }

    inserts_done.store(true);
}

int main(int argc, char **argv) {

    if (argc < 5) {
        fprintf(stderr, "btree_insert_query_tput reccnt query_threads datafile queryfile\n");
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    size_t qthread_cnt = atol(argv[2]);
    std::string d_fname = std::string(argv[3]);
    std::string q_fname = std::string(argv[4]);

    auto tree = new BenchBTree();
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto data = read_sosd_file<Rec>(d_fname, n);
    auto queries = read_range_queries<QP>(q_fname, .001);

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    for (size_t i=0; i<warmup; i++) {
        btree_record<int64_t, int64_t> r;
        r.key = data[i].key;
        r.value = data[i].value;

        tree->insert(r);
    }

    TIMER_INIT();

    std::vector<std::thread> qthreads(qthread_cnt);

    TIMER_START();
    std::thread i_thrd(insert_thread, tree, warmup, &data);
    for (size_t i=0; i<qthread_cnt; i++) {
        qthreads[i] = std::thread(query_thread, tree, &queries);
    }
    i_thrd.join();
    TIMER_STOP();

    for (size_t i=0; i<qthread_cnt; i++) {
        qthreads[i].join();
    }

    auto total_latency = TIMER_RESULT();
    size_t throughput = (size_t) ((double) (n - warmup) / (double) total_latency * 1e9);
    fprintf(stdout, "T\t%ld\t%ld\n", total_latency, throughput);

    gsl_rng_free(rng);
    delete tree;
    fflush(stderr);
}

