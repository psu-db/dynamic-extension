/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/irs.h"
#include "framework/interface/Record.h"
#include "include/file_util.h"
#include <ctime>

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::irs::Query<Rec, ISAM> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;
typedef de::irs::Parms<Rec> QP;

std::atomic<bool> inserts_done = false;

struct timespec delay = {0, 500};

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
        nanosleep(&delay, nullptr);
    }

    fprintf(stderr, "%ld\n", total);

    gsl_rng_free(rng);
}

void insert_thread(Ext *extension, size_t start, size_t stop, std::vector<Rec> *records) {
    fprintf(stderr, "%ld\t%ld\n", start, stop);
    for (size_t i=start; i<stop; i++) {
        while (!extension->insert((*records)[i])) {
            nanosleep(&delay, nullptr);
        }
    }
}

int main(int argc, char **argv) {

    if (argc < 6) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "%s reccnt insert_threads query_threads datafile queryfile\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    size_t ithread_cnt = atol(argv[2]);
    size_t qthread_cnt = atol(argv[3]);
    std::string d_fname = std::string(argv[4]);
    std::string q_fname = std::string(argv[5]);

    auto extension = new Ext(1000, 12000, 8, 0, 64);
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto data = read_sosd_file<Rec>(d_fname, n);
    auto queries = read_range_queries<QP>(q_fname, .001);

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    for (size_t i=0; i<warmup; i++) {
        while (!extension->insert(data[i])) {
            usleep(1);
        }
    }

    extension->await_next_epoch();

    TIMER_INIT();

    std::vector<std::thread> ithreads(ithread_cnt);
    std::vector<std::thread> qthreads(qthread_cnt);

    TIMER_START();
    size_t start = warmup;
    size_t per_thread = (n - warmup) / ithread_cnt;
    for (size_t i=0; i<ithread_cnt; i++) {
        ithreads[i] = std::thread(insert_thread, extension, start, start + per_thread, &data);
        start += per_thread;
    }

    for (size_t i=0; i<qthread_cnt; i++) {
        qthreads[i] = std::thread(query_thread, extension, &queries);
    }

    for (size_t i=0; i<ithread_cnt; i++) {
        ithreads[i].join();
    }

    inserts_done.store(true);
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

