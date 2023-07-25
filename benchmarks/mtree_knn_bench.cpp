#include "include/bench.h"
#include "mtree.h"

static void mtree_knn_bench(MTree &tree, std::vector<de::KNNQueryParms<Word2VecRec>> queries, size_t trial_cnt=1) 
{
    char progbuf[25];
    sprintf(progbuf, "sampling:");

    size_t batch_size = 100;
    size_t batches = trial_cnt / batch_size;
    size_t total_time = 0;

    std::vector<Word2VecRec> result_set;

    for (int i=0; i<trial_cnt; i++) {
        progress_update((double) (i * batch_size) / (double) trial_cnt, progbuf);

        std::vector<Word2VecRec> results;

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j=0; j<queries.size(); j++) {
            results.clear();
            auto query_output = tree.get_nearest_by_limit(queries[j].point, queries[j].k);
            auto itr = query_output.begin();
            while (itr != query_output.end()) {
                results.emplace_back(itr->data);
                itr++;
            }
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
        fprintf(stderr, "Usage: mtree_knn_bench <filename> <record_count> <delete_proportion> <query_file> [k]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double delete_prop = atof(argv[3]);
    std::string qfilename = std::string(argv[4]);
    size_t k = (argc == 6) ? atol(argv[5]) : 10;

    init_bench_env(record_count, true);
    auto queries = read_knn_queries<de::KNNQueryParms<Word2VecRec>>(qfilename, k);

    auto mtree = MTree();

    std::fstream datafile;
    datafile.open(filename, std::ios::in | std::ios::binary);

    std::vector<Word2VecRec> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = 0.1 * record_count;
    warmup<MTree, Word2VecRec>(datafile, mtree, warmup_cnt, delete_prop, to_delete, true, true);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_tput_bench<MTree, Word2VecRec>(mtree, datafile, insert_cnt, delete_prop, to_delete, true);
   // fprintf(stdout, "%ld\t", mtree.get_memory_usage());

    mtree_knn_bench(mtree, queries);
    fprintf(stdout, "\n");

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
