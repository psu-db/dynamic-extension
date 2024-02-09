/*
 *
 */

#define ENABLE_TIMER

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/rangequery.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::rq::Query<Rec, ISAM> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;



int main(int argc, char **argv) {
    std::vector hwms = {5000l, 10000l, 20000l, 50000l};
    std::vector lwms = {.1, .2, .3, .4, .5, .6, .7, .8, .9};

    size_t n = 1000000;

    TIMER_INIT();

    for (auto &hwm : hwms) {
        for (size_t i=0; i<lwms.size(); i++) {
            size_t lwm = hwm * lwms[i];

            auto extension = new Ext(lwm, hwm, 8);
            TIMER_START();
            for (int64_t i=0; i<n; i++) {
                Rec r = {i, i};
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

