/*
 *
 */

#define ENABLE_TIMER

#include "shard/ISAMTree.h"
#include "query/irs.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "benchmark_types.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"
#include "standard_benchmarks.h"
#include "psu-ds/BTree.h"

typedef btree_record<int64_t, int64_t> Rec;

typedef de::ISAMTree<Rec> Shard;
typedef de::irs::Query<Shard> Q;
typedef Q::Parameters QP;

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

    auto btree = BenchBTree();

    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto data = read_sosd_file<Rec>(d_fname, n);
    std::vector<size_t> to_delete(n * delete_proportion);
    size_t j=0;
    for (size_t i=0; i<data.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= delete_proportion) {
            to_delete[j++] = i;
        } 
    }
    /* read in the range queries and add sample size and rng for sampling */
    auto queries = read_range_queries<QP>(q_fname, .0001);
    for (auto &q : queries) {
        q.sample_size = 1000;
        q.rng = rng;
    }

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    size_t delete_idx = 0;
    insert_records<BenchBTree, Rec>(&btree, 0, warmup, data, to_delete, delete_idx, false, rng);

    TIMER_INIT();

    TIMER_START();
    insert_records<BenchBTree, Rec>(&btree, warmup, data.size(), data, to_delete, delete_idx, true, rng);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    TIMER_START();
    run_btree_queries<Rec, Q>(&btree, queries);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    auto btree_size = btree.get_stats().inner_nodes * psudb::btree_default_traits<int64_t, Rec>::inner_slots * (sizeof(int64_t) + sizeof(void*));

    /* account for memory wasted on gaps in the structure */
    btree_size += btree.get_stats().leaves * psudb::btree_default_traits<int64_t, Rec>::leaf_slots * sizeof(Rec);
    btree_size -= btree.size() * sizeof(Rec);

    fprintf(stdout, "%ld\t%ld\t%ld\n", insert_throughput, query_latency, btree_size);

    gsl_rng_free(rng);
    fflush(stderr);
}

