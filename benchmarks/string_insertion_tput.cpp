/*
 *
 */

#define ENABLE_TIMER

#include <fstream>
#include <sstream>

#include "framework/DynamicExtension.h"
#include "shard/FSTrie.h"
#include "query/pointlookup.h"
#include "framework/interface/Record.h"

#include "psu-util/timer.h"
#include "psu-util/progress.h"


typedef de::Record<const char *, uint64_t> Rec;
typedef de::FSTrie<Rec> Trie;
typedef de::pl::Query<Rec, Trie> Q;
typedef de::DynamicExtension<Rec, Trie, Q, de::LayoutPolicy::TEIRING, de::DeletePolicy::TAGGING, de::SerialScheduler> Ext;

std::vector<std::unique_ptr<char[]>> strings;

void insert_thread(int64_t start, int64_t end, Ext *extension) {
    for (uint64_t i=start; i<end; i++) {
            Rec r = {strings[i].get(), i, strlen(strings[i].get())};
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
        strings.emplace_back(std::unique_ptr<char[]>(strdup(line.c_str())));
        i++;
        psudb::progress_update((double) i / (double) n, "Reading file:");
    }
}

void usage(char *name) {
    fprintf(stderr, "Usage:\n%s datafile record_count\n", name);
}

int main(int argc, char **argv) {

    if (argc < 3) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    std::string fname = std::string(argv[1]);
    size_t n = atol(argv[2]);

    read_data(fname, n);

    if (strings.size() == 0) {
        fprintf(stderr, "[E]: No string data read from file. Aborting execution.\n");
    } else {
        fprintf(stderr, "Finished reading from file.\n");
    }

    std::vector<size_t> scale_factors = {2, 4, 6, 8, 10, 12};
    std::vector<size_t> buffer_sizes = {1000, 2000, 5000, 10000, 12000, 15000};

    for (auto &sf : scale_factors) {
        for (auto &bf_sz : buffer_sizes) {

    auto extension = new Ext(bf_sz, bf_sz, sf);

    TIMER_INIT();
    TIMER_START();
    insert_thread(0, strings.size(), extension);
    TIMER_STOP();

    auto total_time = TIMER_RESULT();

    size_t m = 100;
    TIMER_START();
    for (size_t i=0; i<m; i++) {
        size_t j = rand() % strings.size();
        de::pl::Parms<Rec> parms = {strings[j].get()};

        auto res = extension->query(&parms);
        auto ans = res.get();

        if (ans[0].value != j) {
            fprintf(stderr, "ext:\t%ld %ld %s\n", ans[0].value, j, strings[j].get());
        }

        assert(ans[0].value == j);
    }
    TIMER_STOP();

    auto query_time = TIMER_RESULT();
    
    double i_tput = (double) n / (double) total_time * 1e9;
    size_t q_lat = query_time / m;

            fprintf(stdout, "%ld\t%ld\t%ld\t%lf\t%ld\t%ld\n", extension->get_record_count(), 
                    bf_sz, sf, i_tput, q_lat, extension->get_memory_usage());

    delete extension;

        }}
    fflush(stderr);
}

