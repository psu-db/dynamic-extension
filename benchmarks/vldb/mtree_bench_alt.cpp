/*
 *
 */

#define ENABLE_TIMER

#include "query/knn.h"
#include "file_util.h"
#include "standard_benchmarks.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef ANNRec Rec;
typedef de::knn::Parms<Rec> QP;

void usage(char *progname) {
    fprintf(stderr, "%s reccnt datafile queryfile\n", progname);
}

int main(int argc, char **argv) {

    if (argc < 4) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = std::string(argv[2]);
    std::string q_fname = std::string(argv[3]);

    auto mtree = new MTree_alt();
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    fprintf(stderr, "[I] Reading data file...\n");
    auto data = read_binary_vector_file<Rec>(d_fname, n);

    fprintf(stderr, "[I] Generating delete vector\n");
    std::vector<size_t> to_delete(n * delete_proportion);
    size_t j=0;
    for (size_t i=0; i<data.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= delete_proportion) {
            to_delete[j++] = i;
        } 
    }
    fprintf(stderr, "[I] Reading Queries\n");
    auto queries = read_binary_knn_queries<QP>(q_fname, 1000, 100);

    fprintf(stderr, "[I] Warming up structure...\n");
    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    size_t delete_idx = 0;
    insert_records<MTree_alt, Rec>(mtree, 0, warmup, data, to_delete, delete_idx, false, rng);

    TIMER_INIT();

    fprintf(stderr, "[I] Running Insertion Benchmark\n");
    TIMER_START();
    insert_records<MTree_alt, Rec>(mtree, warmup, data.size(), data, to_delete, delete_idx, true, rng);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    fprintf(stderr, "[I] Running Query Benchmark\n");
    TIMER_START();
    run_queries<MTree_alt, QP>(mtree, queries);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    auto size = mtree->size() - sizeof(Rec)*(data.size() - to_delete.size());

    fprintf(stdout, "%ld\t%ld\t%ld\n", insert_throughput, query_latency, size);

    gsl_rng_free(rng);
    delete mtree;
    fflush(stderr);
}

