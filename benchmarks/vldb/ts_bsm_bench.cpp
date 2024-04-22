/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "triespline_bsm.h"
#include "psu-util/bentley-saxe.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "query/rangecount.h"
#include "psu-util/timer.h"
#include "standard_benchmarks.h"

typedef std::pair<uint64_t, uint64_t> Rec;
typedef de::Record<uint64_t, uint64_t> FRec;

typedef BSMTrieSpline<uint64_t, uint64_t> Shard;
typedef de::rc::Parms<FRec> QP;
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

    auto extension = new psudb::bsm::BentleySaxe<Rec, Shard>();
    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto data = read_sosd_file_pair<uint64_t, uint64_t>(d_fname, n);
    auto queries = read_range_queries<QP>(q_fname, .001);

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    insert_records<Shard, Rec>(extension, 0, warmup, data);

    TIMER_INIT();

    TIMER_START();
    insert_records<Shard, Rec>(extension, warmup, data.size(), data);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    TIMER_START();
    run_queries<Ext, QP, true>(extension, queries);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    fprintf(stdout, "%ld\t%ld\n", insert_throughput, query_latency);

    gsl_rng_free(rng);
    delete extension;
    fflush(stderr);
}

