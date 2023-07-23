#include "include/bench.h"
#include "ds/BTree.h"

static void btree_rq_bench(TreeMap &tree, std::vector<de::ISAMRangeQueryParms<btree_record>> queries, size_t trial_cnt=1) 
{
    char progbuf[25];
    sprintf(progbuf, "sampling:");

    size_t batch_size = 100;
    size_t batches = trial_cnt / batch_size;
    size_t total_time = 0;

    std::vector<btree_record> result_set;

    for (int i=0; i<trial_cnt; i++) {
        progress_update((double) (i * batch_size) / (double) trial_cnt, progbuf);

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j=0; j<queries.size(); j++) {
            auto ptr = tree.find(queries[j].lower_bound);
            while (ptr != tree.end() && ptr->key <= queries[j].upper_bound) {
                result_set.emplace_back(*ptr);
                ptr++;
            }
            result_set.clear();
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    progress_update(1.0, progbuf);

    size_t latency = total_time / (trial_cnt * queries.size());

    fprintf(stdout, "%ld\t", latency);
}



int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: btree_rq_bench <filename> <record_count> <delete_proportion> <query_file>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double delete_prop = atof(argv[3]);
    std::string qfilename = std::string(argv[4]);

    size_t buffer_cap = 12000;
    size_t scale_factor = 6;
    double max_delete_prop = delete_prop;
    bool use_osm = false;

    double insert_batch = 0.1; 

    init_bench_env(record_count, true, use_osm);
    auto queries = read_range_queries<de::ISAMRangeQueryParms<btree_record>>(qfilename, .0001);

    auto btree = TreeMap();

    std::fstream datafile;
    datafile.open(filename, std::ios::in | std::ios::binary);

    std::vector<btree_record> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup<TreeMap, btree_record>(datafile, btree, warmup_cnt, delete_prop, to_delete, true, true);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_tput_bench<TreeMap, btree_record>(btree, datafile, insert_cnt, delete_prop, to_delete, true);
    size_t memory_usage = btree.get_stats().inner_nodes * tlx::btree_default_traits<key_type, btree_record>::inner_slots * (sizeof(key_type) + sizeof(void*));
    fprintf(stdout, "%ld\t", memory_usage);

    btree_rq_bench(btree, queries);
    fprintf(stdout, "\n");

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
