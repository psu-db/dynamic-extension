#include "include/bench.h"

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: vptree_knn_bench <filename> <record_count> <delete_proportion> <query_file>\n");
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

    auto de_vp_knn = ExtendedVPTree_KNN(buffer_cap, scale_factor, max_delete_prop);

    std::fstream datafile;
    datafile.open(filename, std::ios::in | std::ios::binary);

    std::vector<Word2VecRec> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup<ExtendedVPTree_KNN, Word2VecRec>(datafile, de_vp_knn, warmup_cnt, delete_prop, to_delete, true, true);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_tput_bench<ExtendedVPTree_KNN, Word2VecRec>(de_vp_knn, datafile, insert_cnt, delete_prop, to_delete, true);
    fprintf(stdout, "%ld\t", de_vp_knn.get_memory_usage());
/*
    query_latency_bench<ExtendedVPTree_KNN, Word2VecRec, de::KNNQueryParms<Word2VecRec>>(de_vp_knn, queries);
    fprintf(stdout, "\n");

    auto ts = de_vp_knn.create_static_structure();

    fprintf(stdout, "%ld\t", ts->get_memory_usage());
    static_latency_bench<de::VPTree<Word2VecRec>, Word2VecRec, de::KNNQueryParms<Word2VecRec>, de::KNNQuery<Word2VecRec>>(
        ts, queries, 1
    );
    fprintf(stdout, "\n");

    delete ts;
    */

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
