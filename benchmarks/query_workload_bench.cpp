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
typedef de::rc::Query<Rec, ISAM> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;

size_t g_insert_size = 50000;
size_t g_insert_frequency = 1000;
size_t g_query_count = 5000;

void query_thread(Ext *extension, gsl_rng *rng, size_t n, bool parallel=true) {
    TIMER_INIT();
    double selectivity = .001;
    size_t k = 100;
    size_t range = n * selectivity;

    size_t total_result = 0;

    auto q = new de::rc::Parms<Rec>();

    std::vector<std::future<std::vector<Rec>>> results(k);

    TIMER_START();
    for (int64_t i=0; i<k; i++) {
        size_t start = gsl_rng_uniform_int(rng, n - range);

        q->lower_bound = start;
        q->upper_bound = start + range;
        results[i] = extension->query(q);
        if (!parallel) {
            auto x = results[i].get();
            total_result += x[0].key;
        }
    }

    if (parallel) {
        for (size_t i=0; i<k; i++) {
            auto x = results[i].get();
            total_result += x[0].key;
        }
    }

    TIMER_STOP();
    auto query_lat = TIMER_RESULT();
    fprintf(stdout, "Q\t%ld\t%ld\t%ld\n", extension->get_record_count(), query_lat, k);
    fprintf(stderr, "Q Total: %ld\n", total_result);
    delete q;
}

void insert_thread(Ext *extension, size_t n) {
    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);

    TIMER_INIT();
    size_t k=1000;

    Rec r;
    for (size_t i=0; i<g_insert_size; i+=k) {
        TIMER_START();
        for (size_t j=0; j<k; j++) {
            r.key = gsl_rng_uniform_int(rng, n);
            r.value = gsl_rng_uniform_int(rng, n);

            while (!extension->insert(r)) {
                _mm_pause();
            }
        }
        TIMER_STOP();

        auto insert_lat = TIMER_RESULT();
        fprintf(stdout, "I\t%ld\t%ld\t%ld\n", extension->get_record_count(), insert_lat, k);
    }

    gsl_rng_free(rng);
}

void parallel_bench(Ext *extension, gsl_rng *rng, size_t n) {
    TIMER_INIT();

    TIMER_START();
    for (size_t i=0; i < g_query_count; i+=100) {
        query_thread(extension, rng, n);
        if (i % g_insert_frequency == 0) {
            auto x = std::thread(insert_thread, extension, n);
            x.detach();
        }
    }
    TIMER_STOP();

    auto workload_duration = TIMER_RESULT();
    fprintf(stdout, "W\t%ld\n", workload_duration);
}


void serial_bench(Ext *extension, gsl_rng *rng, size_t n) {
    TIMER_INIT();
    TIMER_START();
    for (size_t i=0; i < g_query_count; i+=100) {
        query_thread(extension, rng, n, false);
        if (i % g_insert_frequency == 0) {
            auto x = std::thread(insert_thread, extension, n);
            x.join();
        }
    }
    TIMER_STOP();

    auto workload_duration = TIMER_RESULT();
    fprintf(stdout, "W\t%ld\n", workload_duration);
}

int main(int argc, char **argv) {

    if (argc < 5) {
        fprintf(stderr, "query_workload_bench reccnt lwm hwm parallel\n");
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    size_t lwm = atol(argv[2]);
    size_t hwm = atol(argv[3]);
    bool parallel = atoi(argv[4]);

    size_t scale_factor = 8;

    auto extension = new Ext(lwm, hwm, scale_factor);
    size_t per_trial = 1000;
    double selectivity = .001;

    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);

    /* build initial structure */
    size_t reccnt = 0;
    Rec r;
    for (size_t i=0; i<n; i++) {
        r.key = gsl_rng_uniform_int(rng, n);
        r.value = gsl_rng_uniform_int(rng, n);

        while (!extension->insert(r)) {
            _mm_pause();
        }
    }

    if (parallel) {
        parallel_bench(extension, rng, n);
    } else {
        serial_bench(extension, rng, n);
    }

    gsl_rng_free(rng);
    delete extension;
    fflush(stderr);
    fflush(stdout);
}

