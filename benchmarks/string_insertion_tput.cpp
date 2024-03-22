/*
 *
 */

#define ENABLE_TIMER

#include <fstream>
#include <sstream>

#include "framework/DynamicExtension.h"
#include "shard/FSTrie.h"
#include "query/rangequery.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"
#include "psu-util/progress.h"


typedef de::Record<std::string, uint64_t> Rec;
typedef de::FSTrie<Rec> Trie;
typedef de::rq::Query<Rec, Trie> Q;
typedef de::DynamicExtension<Rec, Trie, Q> Ext; //, de::LayoutPolicy::TEIRING, de::DeletePolicy::TAGGING, de::SerialScheduler> Ext;

std::vector<std::string> strings;

void insert_thread(int64_t start, int64_t end, Ext *extension) {
    for (uint64_t i=start; i<end; i++) {
            Rec r = {strings[i], i};
            while (!extension->insert(r)) {
                _mm_pause();
            }
    }
}

void read_data(std::string fname, size_t n=10000000) {
    strings.reserve(n);

    std::fstream file;
    file.open(fname, std::ios::in);

    size_t i=0;
    std::string line;
    while (i < n && std::getline(file, line, '\n')) {
        strings.emplace_back(line);
        i++;
        psudb::progress_update((double) i / (double) n, "Reading file:");
    }
}

int main(int argc, char **argv) {
    size_t n = 100000000;

    std::vector<int> counts = {1 , 2, 4, 8}; //, 16, 32, 64};
    //
    read_data("benchmarks/data/ursa-genome.txt", n);

    fprintf(stderr, "Finished reading from file.\n");

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

