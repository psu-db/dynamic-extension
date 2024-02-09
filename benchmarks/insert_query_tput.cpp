/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/irs.h"
#include "framework/interface/Record.h"
#include "include/data-proc.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::irs::Query<Rec, ISAM> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;
typedef de::irs::Parms<Rec> QP;

std::atomic<bool> inserts_done = false;

void query_thread(Ext *extension, std::vector<QP> *queries) {
    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);
    size_t total = 0;

    while (!inserts_done.load()) {
        auto q_idx = gsl_rng_uniform_int(rng, queries->size());

        auto q = (*queries)[q_idx];
        q.rng = rng;
        q.sample_size = 1000;

        auto res = extension->query(&q);
        auto r = res.get();
        total += r.size();
        usleep(1);
    }

    fprintf(stderr, "%ld\n", total);

    gsl_rng_free(rng);
}

void insert_thread(Ext *extension, size_t start, std::vector<int64_t> *records) {
    size_t reccnt = 0;
    Rec r;
    for (size_t i=start; i<records->size(); i++) {
        r.key = (*records)[i];
        r.value = i;

        while (!extension->insert(r)) {
            usleep(1);
        }
    }

    inserts_done.store(true);
}

int main(int argc, char **argv) {

    if (argc < 5) {
        fprintf(stderr, "insert_query_tput reccnt query_threads datafile queryfile\n");
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    size_t qthread_cnt = atol(argv[2]);
    std::string d_fname = std::string(argv[3]);
    std::string q_fname = std::string(argv[4]);

    auto extension = new Ext(1000, 12000, 8, 0, 64);
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto data = read_sosd_file(d_fname, n);
    auto queries = read_range_queries<QP>(q_fname, .001);

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    Rec r;
    for (size_t i=0; i<warmup; i++) {
        r.key = data[i];
        r.value = gsl_rng_uniform_int(rng, n);

        while (!extension->insert(r)) {
            usleep(1);
        }
    }

    extension->await_next_epoch();

    TIMER_INIT();

    std::vector<std::thread> qthreads(qthread_cnt);

    TIMER_START();
    std::thread i_thrd(insert_thread, extension, warmup, &data);
    for (size_t i=0; i<qthread_cnt; i++) {
        qthreads[i] = std::thread(query_thread, extension, &queries);
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
    delete extension;
    fflush(stderr);
}

