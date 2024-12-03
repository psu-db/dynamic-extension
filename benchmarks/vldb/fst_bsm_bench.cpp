/*
 *
 */

#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/FSTrie.h"
#include "query/pointlookup.h"
#include "framework/interface/Record.h"
#include "file_util.h"
#include "standard_benchmarks.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<const char *, uint64_t> Rec;
typedef de::FSTrie<Rec> Shard;
typedef de::pl::Query<Shard> Q;
typedef de::DynamicExtension<Shard, Q, de::LayoutPolicy::BSM, de::DeletePolicy::TOMBSTONE, de::SerialScheduler> Ext;
typedef Q::Parameters QP;

void usage(char *progname) {
    fprintf(stderr, "%s reccnt datafile\n", progname);
}

int main(int argc, char **argv) {

    if (argc < 3) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = std::string(argv[2]);

    auto extension = new Ext(1, 12001, 2, 0, 64);
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto strings = read_string_file(d_fname, n);
    auto queries = generate_string_lookup_queries<QP>(strings, 1000, rng);

    std::vector<Rec> data;
    for (size_t i=0; i<strings.size(); i++) {
        data.push_back({strings[i].get(), i, strlen(strings[i].get())});
    }

    std::vector<size_t> to_delete(n * delete_proportion);
    size_t j=0;
    for (size_t i=0; i<strings.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= delete_proportion) {
            to_delete[j++] = i;
        } 
    }

    /* warmup structure w/ 10% of records */
    size_t warmup = .1 * n;
    size_t delete_idx = 0;
    insert_records<Ext, Rec>(extension, 0, warmup, data, to_delete, delete_idx, false, rng);

    extension->await_next_epoch();

    TIMER_INIT();

    TIMER_START();
    insert_records<Ext, Rec>(extension, warmup, data.size(), data, to_delete, delete_idx, true, rng);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    TIMER_START();
    run_queries<Ext, Q>(extension, queries);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    auto shard = extension->create_static_structure();

    TIMER_START();
    run_static_queries<Shard, Q>(shard, queries);
    TIMER_STOP();

    auto static_latency = TIMER_RESULT() / queries.size();

    auto ext_size = extension->get_memory_usage() + extension->get_aux_memory_usage();
    auto static_size = shard->get_memory_usage(); //+ shard->get_aux_memory_usage();

    fprintf(stdout, "%ld\t%ld\t%ld\t%ld\t%ld\n", insert_throughput, query_latency, ext_size, static_latency, static_size);

    gsl_rng_free(rng);
    delete extension;
    fflush(stderr);
}

