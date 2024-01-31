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
typedef de::DynamicExtension<Rec, ISAM, Q, de::LayoutPolicy::TEIRING, de::DeletePolicy::TAGGING, de::SerialScheduler> Ext;

std::atomic<bool> inserts_done = false;


void query_thread(Ext *extension, double selectivity, size_t k, gsl_rng *rng) {
    TIMER_INIT();

    size_t reccnt = extension->get_record_count();

    size_t range = reccnt * selectivity;

    auto q = new de::rc::Parms<Rec>();

    TIMER_START();
    for (int64_t i=0; i<k; i++) {
        size_t start = gsl_rng_uniform_int(rng, reccnt - range);

        q->lower_bound = start;
        q->upper_bound = start + range;
        auto res = extension->query(q);
        auto r = res.get();
    }
    TIMER_STOP();
    auto query_lat = TIMER_RESULT();
    fprintf(stdout, "Q\t%ld\t%ld\t%ld\n", reccnt, query_lat, k);
    delete q;
}

void insert_thread(Ext *extension, size_t n, size_t k, gsl_rng *rng) {
    TIMER_INIT();

    size_t reccnt = 0;
    Rec r;
    while (reccnt < n) {
        auto old_reccnt = reccnt;

        TIMER_START();
        for (size_t i=0; i<k; i++) {
            r.key = reccnt;
            r.value = reccnt;

            if (extension->insert(r)) {
                reccnt++;
            }
        }
        TIMER_STOP();
        auto insert_lat = TIMER_RESULT();

        fprintf(stdout, "I\t%ld\t%ld\t%ld\n", reccnt, insert_lat, reccnt - old_reccnt);

        if (reccnt % 100000 == 0 && reccnt != n)  {
            auto a = std::thread(query_thread, extension, .01, 20, rng);
            a.join();
        }
    }
}

int main(int argc, char **argv) {

    /* the closeout routine takes _forever_ ... so we'll just leak the memory */
    auto extension = new Ext(1000, 12000, 8);
    size_t n = 10000000;
    size_t per_trial = 1000;
    double selectivity = .001;

    TIMER_INIT();

    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);

    TIMER_START();
    std::thread i_thrd(insert_thread, extension, n, per_trial, rng);
    i_thrd.join();
    TIMER_STOP();

    auto total_latency = TIMER_RESULT();
    fprintf(stdout, "T\t%ld\n", total_latency);

    gsl_rng_free(rng);
    delete extension;
    fflush(stderr);
}

