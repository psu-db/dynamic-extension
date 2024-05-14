/*
 *
 */

#define ENABLE_TIMER

#include "file_util.h"
#include "benchmark_types.h"

#include <gsl/gsl_rng.h>

typedef ANNRec Rec;

void usage(char *progname) {
    fprintf(stderr, "%s reccnt datafile sampcnt\n", progname);
}

int main(int argc, char **argv) {

    if (argc < 4) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = std::string(argv[2]);
    size_t m = atol(argv[3]);

    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto data = read_binary_vector_file<Rec>(d_fname, n);

    std::vector<size_t> to_delete(m);

    std::unordered_map<Rec, size_t, de::RecordHash<Rec>> filter;
    double ratio = (double) data.size() / (double) m;
    size_t j=0;
    for (size_t i=0; i<data.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= ratio && filter.find(data[i]) == filter.end()) {
            to_delete[j++] = i;
            filter.insert({data[i], i});
        } 
    }

    for (size_t i=0; i<to_delete.size(); i++) {
        for (size_t j=0; j<ANNSize; j++ ) {
            fprintf(stdout, "%ld ", data[to_delete[i]].data[j]);
        }
        fprintf(stdout, "\n");
    }

    gsl_rng_free(rng);
    fflush(stderr);
    fflush(stdout);
}

