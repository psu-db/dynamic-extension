#include "include/bench.h"

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: isam_rq_bench <filename> <record_count> <delete_proportion> <query_file>\n");
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
    auto queries = read_range_queries<de::ISAMRangeQueryParms<Rec>>(qfilename, .0001);

    auto de_isam_rq = ExtendedISAM_RQ(buffer_cap, scale_factor, max_delete_prop);

    std::fstream datafile;
    datafile.open(filename, std::ios::in | std::ios::binary);

    std::vector<Rec> to_delete;

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup<ExtendedISAM_RQ, Rec>(datafile, de_isam_rq, warmup_cnt, delete_prop, to_delete, true, true);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_tput_bench<ExtendedISAM_RQ, Rec>(de_isam_rq, datafile, insert_cnt, delete_prop, to_delete, true);
    fprintf(stdout, "%ld\t", de_isam_rq.get_memory_usage());
    query_latency_bench<ExtendedISAM_RQ, Rec, de::ISAMRangeQueryParms<Rec>>(de_isam_rq, queries);
    fprintf(stdout, "\n");

    auto ts = de_isam_rq.create_static_structure();

    fprintf(stdout, "%ld\t", ts->get_memory_usage());
    static_latency_bench<de::MemISAM<Rec>, Rec, de::ISAMRangeQueryParms<Rec>, de::ISAMRangeQuery<Rec>>(
        ts, queries, 1
    );
    fprintf(stdout, "\n");

    delete ts;

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
