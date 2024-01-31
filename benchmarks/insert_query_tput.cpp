/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::rc::Query<ISAM, Rec> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;

std::atomic<bool> inserts_done = false;

void query_thread(Ext *extension, size_t n) {
    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);
    size_t range = n*.0001;

    size_t total = 0;

    de::rc::Parms<Rec> *q = new de::rc::Parms<Rec>();
    while (!inserts_done.load()) {
        size_t start = gsl_rng_uniform_int(rng, n - range);
        q->lower_bound = start;
        q->upper_bound = start + range;
        auto res = extension->query(q);
        auto r = res.get();
	total += r[0].key;
	usleep(1);
    }

    fprintf(stderr, "%ld\n", total);

    gsl_rng_free(rng);
    delete q;
}

void insert_thread(Ext *extension, size_t n, gsl_rng *rng) {
    size_t reccnt = 0;
    Rec r;
    for (size_t i=0; i<n; i++) {
        r.key = gsl_rng_uniform_int(rng, n);
        r.value = gsl_rng_uniform_int(rng, n);

        while (!extension->insert(r)) {
            usleep(1);
        }
    }

    inserts_done.store(true);
}

int main(int argc, char **argv) {

    if (argc < 3) {
        fprintf(stderr, "insert_query_tput reccnt query_threads\n");
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    size_t qthread_cnt = atol(argv[2]);

    auto extension = new Ext(1000, 12000, 8, 0, 64);
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    Rec r;
    for (size_t i=0; i<warmup; i++) {
        r.key = gsl_rng_uniform_int(rng, n);
        r.value = gsl_rng_uniform_int(rng, n);

        while (!extension->insert(r)) {
            usleep(1);
        }
    }

    extension->await_next_epoch();

    TIMER_INIT();

    std::vector<std::thread> qthreads(qthread_cnt);

    TIMER_START();
    std::thread i_thrd(insert_thread, extension, n - warmup, rng);
    for (size_t i=0; i<qthread_cnt; i++) {
        qthreads[i] = std::thread(query_thread, extension, n);
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

