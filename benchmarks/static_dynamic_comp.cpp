/*
 *
 */

#define ENABLE_TIMER

#include "framework/DynamicExtension.h"
#include "query/rangecount.h"
#include "shard/TrieSpline.h"
#include "shard/ISAMTree.h"


#include "framework/interface/Record.h"
#include "framework/interface/Query.h"
#include "include/data-proc.h"

#include "psu-util/timer.h"


typedef de::Record<key_type, value_type> Rec;
typedef de::ISAMTree<Rec> ISAM;
typedef de::TrieSpline<Rec> TS;

typedef de::rc::Query<ISAM, Rec> Q;
typedef de::DynamicExtension<Rec, ISAM, Q> Ext;

typedef de::MutableBuffer<Rec> Buffer;

typedef de::rc::Parms<Rec> query;

Buffer *file_to_mbuffer(std::string &fname, size_t n) {
    std::fstream file;
    file.open(fname, std::ios::in);

    auto buff = new Buffer(n, n+1);

    Rec rec;
    while (next_record(file, rec) && buff->get_record_count() < n) {
        buff->append(rec);    
    }

    return buff;
}

BenchBTree *file_to_btree(std::string &fname, size_t n) {
    std::fstream file;
    file.open(fname, std::ios::in);

    auto btree = new BenchBTree();
    Rec rec;
    while (next_record(file, rec) && btree->size() < n) {
        btree->insert({rec.key, rec.value});
    }

    return btree;
}

template<de::ShardInterface S>
void benchmark_shard(S *shard, std::vector<query> &queries) {
    TIMER_INIT();

    TIMER_START();
    for (auto & q : queries) {
        auto state = de::rc::Query<S, Rec>::get_query_state(shard, &q);
        auto res = de::rc::Query<S, Rec>::query(shard, state, &q);
    } 
    TIMER_STOP();

    auto latency = TIMER_RESULT() / queries.size();
    fprintf(stdout, "%ld %ld\n", latency, shard->get_memory_usage() - shard->get_record_count() * sizeof(de::Wrapped<Rec>));
}

void benchmark_btree(BenchBTree *btree, std::vector<query> &queries) {
    TIMER_INIT();

    TIMER_START();
    for (auto & q : queries) {
        size_t c = 0;
        auto ptr = btree->find(q.lower_bound);
        while(ptr != btree->end() && ptr->key <= q.upper_bound) {
            c++;
        }
    } 
    TIMER_STOP();

    auto latency = TIMER_RESULT() / queries.size();
    auto mem = btree->get_stats().inner_nodes * psudb::btree_default_traits<key_type, btree_record>::inner_slots * (sizeof(key_type) + sizeof(void*));
    fprintf(stdout, "%ld %ld\n", latency, mem); 
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: static_dynamic_comp <filename> <record_count> <query_file>\n");
        exit(EXIT_FAILURE);
    }

    std::string d_fname = std::string(argv[1]);
    size_t reccnt = atol(argv[2]);
    std::string q_fname = std::string(argv[3]);

    init_bench_env(reccnt, true, false);
    auto queries = read_range_queries<query>(q_fname, .001);

    auto buff = file_to_mbuffer(d_fname, reccnt);

    TS *ts = new TS(buff->get_buffer_view());
    benchmark_shard<TS>(ts, queries);
    delete ts;

    ISAM  *isam = new ISAM(buff->get_buffer_view());
    benchmark_shard<ISAM>(isam, queries);
    delete isam;

    auto btree = file_to_btree(d_fname, reccnt);

}

