/*
 *
 */

#define ENABLE_TIMER

#include "framework/DynamicExtension.h"
#include "shard/TrieSpline.h"
#include "query/rangequery.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"

#include <algorithm>
#include <random>

typedef uint64_t K;
typedef de::Record<K, K> Rec;
typedef de::TrieSpline<Rec> ISAM;
typedef de::rq::Query<Rec, ISAM> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;



int main(int argc, char **argv) {
    std::vector hwms = {5000l, 10000l, 20000l, 50000l};
    std::vector lwms = {.1, .2, .3, .4, .5, .6, .7, .8, .9};

    size_t n = 1000000000;

    std::vector<K> keys(n);
    for (K i=0; i<n; i++) {
        keys[i] = i;
    }

    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(keys.begin(), keys.end(), g);

    TIMER_INIT();

    for (auto &hwm : hwms) {
        for (size_t i=0; i<lwms.size(); i++) {
            size_t lwm = hwm * lwms[i];

            auto extension = new Ext(lwm, hwm, 8);
            TIMER_START();
            for (size_t i=0; i<n; i++) {
                Rec r = {keys[i], keys[i]};
                while (!extension->insert(r)) {
                    _mm_pause();
                }
            }
            TIMER_STOP();

            auto insert_time = TIMER_RESULT();
            double insert_throughput = (double) n / (double) insert_time * 1e9;

            fprintf(stdout, "%ld\t%ld\t%lf\n", lwm, hwm, insert_throughput);

            extension->print_scheduler_statistics();

            delete extension;
        }
    }
}

