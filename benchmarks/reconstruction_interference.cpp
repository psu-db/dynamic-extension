/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangequery.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::rq::Query<ISAM, Rec> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;

void query_thread(Ext *extension, double selectivity, size_t k) {
    TIMER_INIT();

    size_t reccnt = extension->get_record_count();
    size_t range = reccnt * selectivity;

    auto q = new de::rq::Parms<Rec>();

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

Ext *build_structure(size_t n) {
    auto extension = new Ext(1000, 10000, 2);

    size_t i=0;
    Rec r;
    do {
        r.key = rand() % n;
        r.value = i;
        if (extension->insert(r)) {
            i++;
        } else {
            _mm_pause();
        }
    } while (i < n);

    extension->await_next_epoch();
    return extension;
}

void query_benchmark(double selectivity, size_t k, Ext *extension) {
    TIMER_INIT();

    size_t query_thrd_cnt = 4;
    std::vector<std::thread> thrds(query_thrd_cnt);

    TIMER_START();
    for (size_t i=0; i<query_thrd_cnt; i++) {
        thrds[i] = std::thread(query_thread, extension, selectivity, k);
    }

    for (size_t i=0; i<query_thrd_cnt; i++) {
       thrds[i].join(); 
    }
    TIMER_STOP();

    auto query_lat = TIMER_RESULT();
    fprintf(stdout, "Q\t%ld\t%ld\t%ld\t%ld\n", extension->get_record_count(), query_lat, k, query_thrd_cnt);
}

int main(int argc, char **argv) {

    /* the closeout routine takes _forever_ ... so we'll just leak the memory */
    size_t n = 10000000;

    size_t per_trial = 1000;
    double selectivity = .001;

    /* build initial structure */
    auto extension = build_structure(n);

    /* benchmark queries w/o any interference from reconstructions */
    query_benchmark(selectivity, per_trial, extension);

    fprintf(stderr, "Running interference test...\n");

    /* trigger a worst-case reconstruction and benchmark the queries */
    std::thread q_thrd(query_benchmark, selectivity, per_trial, extension);
    auto s = extension->create_static_structure();
    fprintf(stderr, "Construction complete\n");
    q_thrd.join();
    
    delete extension;
    delete s;

    fflush(stderr);
}

