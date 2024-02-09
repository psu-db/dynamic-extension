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


void insert_thread(int64_t start, int64_t end, Ext *extension) {
    for (int64_t i=start; i<end; i++) {
            Rec r = {i, i};
            while (!extension->insert(r)) {
                _mm_pause();
            }
    }
}


int main(int argc, char **argv) {


    size_t n = 1000000000;

    std::vector<int> counts = {1, 2, 4, 8}; //, 16, 32, 64};


    for (auto thread_count : counts) {

        auto extension = new Ext(1000, 12000, 8);

        size_t per_thread = n / thread_count;

        std::thread threads[thread_count];

        TIMER_INIT();
        TIMER_START();
        for (size_t i=0; i<thread_count; i++) {
            threads[i] = std::thread(insert_thread, i*per_thread, 
                                     i*per_thread+per_thread, extension);
        }

        for (size_t i=0; i<thread_count; i++) {
            threads[i].join();
        }

        TIMER_STOP();

        auto total_time = TIMER_RESULT();

        double tput = (double) n / (double) total_time * 1e9;

        fprintf(stdout, "%ld\t%d\t%lf\n", extension->get_record_count(), 
                thread_count, tput);

        delete extension;
    }

    fflush(stderr);
}

