/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"
#include <unistd.h>
#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::rc::Query<ISAM, Rec> Q;
typedef de::DynamicExtension<Rec, ISAM, Q, de::LayoutPolicy::TEIRING, de::DeletePolicy::TAGGING, de::FIFOScheduler> Ext;

std::atomic<size_t> total_latency = 0;

void insert_thread(Ext *extension, size_t n, size_t k, size_t rate) {
    int64_t delay = (1.0 / (double) rate) * 10e6; // delay in us
    TIMER_INIT();
    for (int64_t i=0; i<n; i+=k) {
        TIMER_START();
        for (int64_t j=0; j<k; j++) {
            Rec r = {i+j, i+j};
            while (!extension->insert(r)) {
                _mm_pause();
            }

            //usleep(delay);
            /*
            for (size_t i=0; i<10000; i++) {
                __asm__ __volatile__ ("":::"memory");
            }
            */
        }
        TIMER_STOP();

        auto insert_lat =  TIMER_RESULT();

        total_latency.fetch_add(insert_lat);
        fprintf(stdout, "I\t%ld\t%ld\t%ld\n", i+k, insert_lat, k);
    }
}

int main(int argc, char **argv) {

    /* the closeout routine takes _forever_ ... so we'll just leak the memory */
    auto extension = new Ext(100, 1000000, 3);
    size_t n = 100000000;
    size_t per_trial = 1000;
    double selectivity = .001;
    size_t rate = 1000000;

    total_latency.store(0);

    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);

    std::thread i_thrd1(insert_thread, extension, n/2, per_trial, rate);
    std::thread i_thrd2(insert_thread, extension, n/2, per_trial, rate);

    i_thrd1.join();
    i_thrd2.join();

    auto avg_latency = total_latency.load() / n;
    auto throughput = (int64_t) ((double) n / (double) total_latency * 1e9);

    fprintf(stdout, "AVG LAT: %ld\nThroughput: %ld\n", avg_latency, throughput);

    gsl_rng_free(rng);
    fflush(stderr);
}

