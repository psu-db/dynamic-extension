#ifndef H_BENCH
#define H_BENCH
#include "framework/DynamicExtension.h"
#include "shard/WSS.h"
#include "shard/MemISAM.h"
#include "shard/PGM.h"
#include "shard/TrieSpline.h"
#include "shard/WIRS.h"

#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <set>
#include <string>
#include <random>

typedef uint64_t key_type;
typedef uint32_t value_type;
typedef uint64_t weight_type;

typedef de::WeightedRecord<key_type, value_type, weight_type> WRec;
typedef de::Record<key_type, value_type> Rec;

typedef de::DynamicExtension<WRec, de::WSS<WRec>, de::WSSQuery<WRec>> ExtendedWSS;
typedef de::DynamicExtension<Rec, de::TrieSpline<Rec>, de::TrieSplineRangeQuery<Rec>> ExtendedTS;
typedef de::DynamicExtension<Rec, de::PGM<Rec>, de::PGMRangeQuery<Rec>> ExtendedPGM;

static gsl_rng *g_rng;
static std::set<WRec> *g_to_delete;
static bool g_osm_data;

static key_type g_min_key = UINT64_MAX;
static key_type g_max_key = 0;

static size_t g_max_record_cnt = 0;
static size_t g_reccnt = 0;

static constexpr unsigned int DEFAULT_SEED = 0;

static unsigned int get_random_seed()
{
    unsigned int seed = 0;
    std::fstream urandom;
    urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
    urandom.read((char *) &seed, sizeof(seed));
    urandom.close();

    return seed;
}

static key_type osm_to_key(const char *key_field) {
    double tmp_key = (atof(key_field) + 180) * 10e6;
    return (key_type) tmp_key;
}

static void init_bench_rng(unsigned int seed, const gsl_rng_type *type) 
{
    g_rng = gsl_rng_alloc(type);
    gsl_rng_set(g_rng, seed);
}

static void init_bench_env(size_t max_reccnt, bool random_seed, bool osm_correction=true)
{
    unsigned int seed = (random_seed) ? get_random_seed() : DEFAULT_SEED;
    init_bench_rng(seed, gsl_rng_mt19937);
    g_to_delete = new std::set<WRec>();
    g_osm_data = osm_correction;
    g_max_record_cnt = max_reccnt;
    g_reccnt = 0;
}

static void delete_bench_env()
{
    gsl_rng_free(g_rng);
    delete g_to_delete;
}

template <de::RecordInterface R>
static bool next_record(std::fstream &file, R &record)
{
    if (g_reccnt >= g_max_record_cnt) return false;

    std::string line;
    if (std::getline(file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;
        std::string weight_field;

        std::getline(line_stream, value_field, '\t');
        std::getline(line_stream, key_field, '\t');
        std::getline(line_stream, weight_field, '\t');

        record.key = (g_osm_data) ? osm_to_key(key_field.c_str()) : atol(key_field.c_str());
        record.value = atol(value_field.c_str());

        if constexpr (de::WeightedRecordInterface<R>) {
            record.weight = atof(weight_field.c_str());
        }

        if (record.key < g_min_key) g_min_key = record.key;

        if (record.key > g_max_key) g_max_key = record.key;

        g_reccnt++;

        return true;
    }

    return false;
}

template <de::RecordInterface R>
static bool build_insert_vec(std::fstream &file, std::vector<R> &vec, size_t n, 
                             double delete_prop, std::vector<R> &to_delete) {
    vec.clear();
    for (size_t i=0; i<n; i++) {
        R rec;
        if (!next_record(file, rec)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.emplace_back(rec);

        if (gsl_rng_uniform(g_rng) < delete_prop + (delete_prop * .1)) {
            to_delete.emplace_back(rec);
        }
    }

    return true;
}

template <de::RecordInterface R>
static bool build_delete_vec(std::vector<R> &to_delete, std::vector<R> &vec, size_t n) {
    vec.clear();

    size_t cnt = 0;
    while (cnt < n) {
        if (to_delete.size() == 0) { 
            return false;
        }

        auto i = gsl_rng_uniform_int(g_rng, to_delete.size());
        vec.emplace_back(to_delete[i]);
        to_delete.erase(to_delete.begin() + i);
    }
td:
    return true;
}

/*
 * helper routines for displaying progress bars to stderr
 */
static const char *g_prog_bar = "======================================================================";
static const size_t g_prog_width = 50;

static void progress_update(double percentage, std::string prompt) {
    int val = (int) (percentage * 100);
    int lpad = (int) (percentage * g_prog_width);
    int rpad = (int) (g_prog_width - lpad);
    fprintf(stderr, "\r(%3d%%) %20s [%.*s%*s]", val, prompt.c_str(), lpad, g_prog_bar, rpad, "");
    fflush(stderr);   

    if (percentage >= 1) fprintf(stderr, "\n");
}

template <typename DE, de::RecordInterface R>
static bool warmup(std::fstream &file, DE &extended_index, size_t count, 
                   double delete_prop, std::vector<R> to_delete, bool progress=true) {
    size_t batch = std::min(.1 * count, 25000.0);

    std::vector<R> insert_vec;
    std::vector<R> delete_vec;
    insert_vec.reserve(batch);
    delete_vec.reserve(batch*delete_prop);

    size_t inserted = 0;
    size_t delete_idx = 0;
    
    double last_percent = 0;
    while (inserted < count) {
        // Build vector of records to insert and potentially delete
        auto continue_warmup = build_insert_vec(file, insert_vec, batch, delete_prop, to_delete);
        if (inserted > batch) {
            build_delete_vec(to_delete, delete_vec, batch*delete_prop);
            delete_idx = 0;
        }

        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (delete_idx < delete_vec.size() && gsl_rng_uniform(g_rng) < delete_prop) {
                extended_index.erase(delete_vec[delete_idx++]);
            }

            // insert the record;
            extended_index.insert(insert_vec[i]);
            inserted++;

            if (progress) {
                progress_update((double) inserted / (double) count, "warming up:");
            }
        }
    }

    /*
    if (progress) {
        progress_update(1, "warming up:");
    }
    */

    return true;
}


static void reset_de_perf_metrics() {

    /*
     * rejection counters are zeroed automatically by the
     * sampling function itself.
     */

    RESET_IO_CNT(); 
}

#endif // H_BENCH