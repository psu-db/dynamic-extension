/*
 *
 */

#define ENABLE_TIMER

#include "framework/DynamicExtension.h"
#include "shard/VPTree.h"
#include "query/knn.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"

constexpr size_t D = 100;

typedef de::EuclidPoint<int64_t, D> Rec;
typedef de::VPTree<Rec> Shard;
typedef de::knn::Query<Rec, Shard> Q;
typedef de::DynamicExtension<Rec, Shard, Q> Ext;

int main(int argc, char **argv) {
    std::vector hwms = {1000l, 2000l, 4000l, 10000l};
    std::vector lwms = {.1, .2, .3, .4, .5, .6, .7, .8, .9};

    size_t n = 1000000;

    std::vector<Rec> records(n);
    for (size_t i=0; i<n; i++) {
        Rec r;
        for (size_t j=0; j<D; j++) {
            r.data[j] = rand() % n;
        }
    }

    TIMER_INIT();

    for (auto &hwm : hwms) {
        for (size_t i=0; i<lwms.size(); i++) {
            size_t lwm = hwm * lwms[i];

            auto extension = new Ext(lwm, hwm, 8);
            TIMER_START();
            for (int64_t i=0; i<n; i++) {
                while (!extension->insert(records[i])) {
                    _mm_pause();
                }
            }
            TIMER_STOP();

            auto insert_time = TIMER_RESULT();
            double insert_throughput = (double) n / (double) insert_time * 1e9;

            fprintf(stdout, "%ld\t%ld\t%lf\n", lwm, hwm, insert_throughput);
            extension->print_scheduler_statistics();

            fflush(stdout);
            delete extension;
        }
    }
}

