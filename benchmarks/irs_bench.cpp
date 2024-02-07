/*
 *
 */

#define ENABLE_TIMER

#include <thread>

#include "framework/DynamicExtension.h"
#include "shard/ISAMTree.h"
#include "query/irs.h"
#include "framework/interface/Record.h"
#include "include/data-proc.h"

#include <gsl/gsl_rng.h>

#include "psu-util/timer.h"


typedef de::Record<int64_t, int64_t> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::irs::Query<Rec, ISAM> Q;
typedef de::DynamicExtension<Rec, ISAM, Q, de::LayoutPolicy::TEIRING, de::DeletePolicy::TOMBSTONE, de::SerialScheduler> Ext;
typedef de::irs::Parms<Rec> QP;

void run_queries(Ext *extension, std::vector<QP> &queries, gsl_rng *rng) {
    size_t total;
    for (size_t i=0; i<queries.size(); i++) {
        auto q = &queries[i];
        q->rng = rng;
        q->sample_size = 1000;

        auto res = extension->query(q);
        auto r = res.get();
        total += r.size();
    }

    fprintf(stderr, "%ld\n", total);
}

size_t g_deleted_records = 0;
double delete_proportion = 0.05;

void insert_records(Ext *extension, size_t start, 
                    size_t stop,
                    std::vector<int64_t> &records,
                    std::vector<size_t> &to_delete,
                    size_t &delete_idx,
                    bool delete_records,
                    gsl_rng *rng) {
    size_t reccnt = 0;
    Rec r;
    for (size_t i=start; i<stop; i++) {
        r.key = records[i];
        r.value = i;

        while (!extension->insert(r)) {
            usleep(1);
        }

        if (delete_records && gsl_rng_uniform(rng) <= delete_proportion && to_delete[delete_idx] <= i) {
            r.key = records[to_delete[delete_idx]];
            r.value = (int64_t) (to_delete[delete_idx]);
            while (!extension->erase(r)) {
                usleep(1);
            }
            delete_idx++;
            g_deleted_records++;
        }
    }
}

int main(int argc, char **argv) {

    if (argc < 4) {
        fprintf(stderr, "insert_query_tput reccnt datafile queryfile\n");
        exit(EXIT_FAILURE);
    }

    size_t n = atol(argv[1]);
    std::string d_fname = std::string(argv[2]);
    std::string q_fname = std::string(argv[3]);

    auto extension = new Ext(12000, 12001, 8, 0, 64);
    gsl_rng * rng = gsl_rng_alloc(gsl_rng_mt19937);
    
    auto data = read_sosd_file(d_fname, n);
    std::vector<size_t> to_delete(n * delete_proportion);
    size_t j=0;
    for (size_t i=0; i<data.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= delete_proportion) {
            to_delete[j++] = i;
        } 
    }
    auto queries = read_range_queries<QP>(q_fname, .001);

    /* warmup structure w/ 10% of records */
    size_t warmup = .3 * n;
    size_t delete_idx = 0;
    insert_records(extension, 0, warmup, data, to_delete, delete_idx, false, rng);

    extension->await_next_epoch();

    TIMER_INIT();

    TIMER_START();
    insert_records(extension, warmup, data.size(), data, to_delete, delete_idx, true, rng);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (n - warmup) / (double) insert_latency * 1e9);

    TIMER_START();
    run_queries(extension, queries, rng);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    fprintf(stdout, "T\t%ld\t%ld\t%ld\n", insert_throughput, query_latency, g_deleted_records);

    gsl_rng_free(rng);
    delete extension;
    fflush(stderr);
}

