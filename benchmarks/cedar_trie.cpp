/*
 *
 */

#define ENABLE_TIMER

#include <fstream>
#include <sstream>
#include <vector>

#include "cedar.h"

#include "psu-util/timer.h"
#include "psu-util/progress.h"

std::vector<std::string> strings;

typedef cedar::da<int> Trie;

void insert_thread(int64_t start, int64_t end, Trie * trie) {
    for (uint64_t i=start; i<end; i++) {
        auto res = trie->update(strings[i].c_str(), strings[i].size(), i+1);
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

    auto trie = new Trie();

    TIMER_INIT();
    TIMER_START();
    insert_thread(0, strings.size(), trie);
    TIMER_STOP();

    auto total_time = TIMER_RESULT();

    size_t m = 100;
    TIMER_START();
    for (size_t i=0; i<m; i++) {
        size_t j = rand() % strings.size();

        auto res = trie->exactMatchSearch<int>(strings[j].c_str());
        //assert(*(res)+1 == j);
    }
    TIMER_STOP();

    auto query_time = TIMER_RESULT();
    

    double i_tput = (double) n / (double) total_time * 1e9;
    size_t q_lat = query_time / m;

    fprintf(stdout, "%ld\t\t%lf\t%ld\n", trie->size(), 
            i_tput, q_lat);

    fprintf(stdout, "%ld\n", trie->total_size());

    delete trie;

    fflush(stderr);
}

