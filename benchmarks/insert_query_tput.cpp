/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangecount.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::rc::Query<ISAM, Rec> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;

std::atomic<bool> inserts_done = false;

void insert_thread(Ext *extension, size_t n, size_t k) {
    TIMER_INIT();
    for (int64_t i=0; i<n; i+=k) {
        TIMER_START();
        for (int64_t j=0; j<k; j++) {
            Rec r = {i+j, i+j};
            while (!extension->insert(r)) {
                _mm_pause();
            }
        }
        TIMER_STOP();
        auto insert_lat = TIMER_RESULT();

        fprintf(stdout, "I\t%ld\t%ld\t%ld\n", extension->get_record_count(), insert_lat, k);
    }

    inserts_done.store(true);
}

void query_thread(Ext *extension, double selectivity, size_t k) {
    TIMER_INIT();

    while (!inserts_done.load()) {
        size_t reccnt = extension->get_record_count();
        size_t range = reccnt * selectivity;

        auto q = new de::rc::Parms<Rec>();

        TIMER_START();
        for (int64_t i=0; i<k; i++) {
            size_t start = rand() % (reccnt - range);
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
}

int main(int argc, char **argv) {

    /* the closeout routine takes _forever_ ... so we'll just leak the memory */
    auto extension = new Ext(1000, 10000, 2);
    size_t n = 10000000;
    size_t per_trial = 1000;
    double selectivity = .001;

    std::thread i_thrd(insert_thread, extension, n, per_trial);
    std::thread q_thrd(query_thread, extension, selectivity, 1);

    q_thrd.join();
    i_thrd.join();
    fflush(stderr);
}

