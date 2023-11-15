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
typedef de::rq::Query<ISAM, Rec> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;



int main(int argc, char **argv) {

    auto extension = Ext(1000, 2, 1);

    size_t n = 1000000;
    size_t per_trial = 100;

    std::vector<int64_t> latencies;

    TIMER_INIT();
    for (int64_t i=0; i<n; i+=per_trial) {
        TIMER_START();
        for (int64_t j=0; j<per_trial; j++) {
            Rec r = {i+j, i+j};
            extension.insert(r);
        }
        TIMER_STOP();

        auto res = TIMER_RESULT();

        latencies.push_back(TIMER_RESULT());
    }

    for (size_t i=0; i<latencies.size(); i++) {
        fprintf(stdout, "%ld\t%ld\t%ld\n", (1+i)*per_trial, latencies[i], per_trial);
    }
}

