#include "include/bench.h"
#include "mtree.h"

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: mtree_knn_bench <filename> <record_count> <delete_proportion> <query_file>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double delete_prop = atof(argv[3]);
    std::string qfilename = std::string(argv[4]);

    size_t buffer_cap = 12000;
    size_t scale_factor = 6;
    double max_delete_prop = delete_prop;

    double insert_batch = 0.1; 

    init_bench_env(record_count, true);
    auto queries = read_knn_queries<de::KNNQueryParms<Word2VecRec>>(qfilename, 50);

    auto mtree = MTree();

    std::fstream datafile;
    datafile.open(filename, std::ios::in | std::ios::binary);

    std::vector<Word2VecRec> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup<MTree, Word2VecRec>(datafile, mtree, warmup_cnt, delete_prop, to_delete, true, true);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_tput_bench<MTree, Word2VecRec>(mtree, datafile, insert_cnt, delete_prop, to_delete, true);
    //fprintf(stdout, "%ld\t", mtree.get_memory_usage());

//    query_latency_bench<MTree, Word2VecRec, de::KNNQueryParms<Word2VecRec>>(mtree, queries);
 //   fprintf(stdout, "\n");

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
