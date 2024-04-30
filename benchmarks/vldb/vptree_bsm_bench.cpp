/*
 *
 */

#define ENABLE_TIMER

#include "vptree_bsm.h"
#include "file_util.h"
#include "standard_benchmarks.h"
#include "query/knn.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef Word2VecRec Rec;

typedef BSMVPTree<Rec, 100> Shard;
typedef de::knn::Parms<Rec> QP;
typedef psudb::bsm::BentleySaxe<Rec, Shard> Ext;

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

    auto extension = new Ext();
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    fprintf(stderr, "[I] Reading data file...\n");
    auto data = read_vector_file<Rec, 300>(d_fname, n);
    auto queries = read_knn_queries<QP>(q_fname, 1000);

    fprintf(stderr, "[I] Warming up structure...\n");
    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    insert_records<Shard, Rec>(extension, 0, warmup, data);

    TIMER_INIT();

    fprintf(stderr, "[I] Running Insertion Benchmark\n");
    TIMER_START();
    insert_records<Shard, Rec>(extension, warmup, data.size(), data);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    fprintf(stderr, "[I] Running Query Benchmark\n");
    TIMER_START();
    run_queries<Ext, QP, true>(extension, queries);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    fprintf(stdout, "%ld\t%ld\n", insert_throughput, query_latency);

    gsl_rng_free(rng);
    delete extension;
    fflush(stderr);
    fflush(stdout);
}

