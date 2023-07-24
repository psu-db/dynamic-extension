/*
 * benchmarks/include/bench_utility.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include "framework/DynamicExtension.h"
#include "shard/WSS.h"
#include "shard/MemISAM.h"
#include "shard/PGM.h"
#include "shard/TrieSpline.h"
#include "shard/WIRS.h"
#include "ds/BTree.h"
#include "shard/VPTree.h"
#include "mtree.h"

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
typedef uint64_t value_type;
typedef uint64_t weight_type;

typedef de::WeightedRecord<key_type, value_type, weight_type> WRec;
typedef de::Record<key_type, value_type> Rec;

const size_t W2V_SIZE = 300;
typedef de::CosinePoint<double, W2V_SIZE> Word2VecRec;

typedef de::DynamicExtension<WRec, de::WSS<WRec>, de::WSSQuery<WRec>> ExtendedWSS;
typedef de::DynamicExtension<Rec, de::TrieSpline<Rec>, de::TrieSplineRangeQuery<Rec>> ExtendedTSRQ;
typedef de::DynamicExtension<Rec, de::PGM<Rec>, de::PGMRangeQuery<Rec>> ExtendedPGMRQ;
typedef de::DynamicExtension<Rec, de::MemISAM<Rec>, de::IRSQuery<Rec>> ExtendedISAM_IRS;
typedef de::DynamicExtension<Rec, de::MemISAM<Rec>, de::ISAMRangeQuery<Rec>> ExtendedISAM_RQ;
typedef de::DynamicExtension<Word2VecRec, de::VPTree<Word2VecRec>, de::KNNQuery<Word2VecRec>> ExtendedVPTree_KNN;

struct btree_record {
    key_type key;
    value_type value;

   inline bool operator<(const btree_record& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }

    inline bool operator==(const btree_record& other) const {
        return key == other.key && value == other.value;
    }
};

struct btree_key_extract {
    static const key_type &get(const btree_record &v) {
        return v.key;
    }
};


struct cosine_similarity {
    double operator()(const Word2VecRec &first, const Word2VecRec &second) const {
        double prod = 0;
        double asquared = 0;
        double bsquared = 0;

        for (size_t i=0; i<W2V_SIZE; i++) {
            prod += first.data[i] * second.data[i];
            asquared += first.data[i]*first.data[i];
            bsquared += second.data[i]*second.data[i];
        }

        return prod / std::sqrt(asquared * bsquared);
    }
};

typedef tlx::BTree<key_type, btree_record, btree_key_extract> TreeMap;
typedef mt::mtree<Word2VecRec, cosine_similarity> MTree;

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

/*
 * NOTE: The QP type must have lower_bound and upper_bound attributes, which
 * this function will initialize. Any other query parameter attributes must
 * be manually initialized after the call.
 */
template <typename QP>
static std::vector<QP> read_range_queries(std::string fname, double selectivity) {
    std::vector<QP> queries;

    FILE *qf = fopen(fname.c_str(), "r");
    size_t start, stop;
    double sel;
    while (fscanf(qf, "%zu%zu%lf\n", &start, &stop, &sel) != EOF) {
        if (start < stop && std::abs(sel - selectivity) < 0.1) {
            QP q;
            q.lower_bound = start;
            q.upper_bound = stop;
            queries.push_back(q);
        }
    }
    fclose(qf);

    return queries;
}

template <typename QP>
static std::vector<QP> read_knn_queries(std::string fname, size_t k) {
    std::vector<QP> queries;

    FILE *qf = fopen(fname.c_str(), "r");
    char *line = NULL;
    size_t len = 0;

    while (getline(&line, &len, qf) > 0) {
        char *token;
        QP query;
        size_t idx = 0;

        token = strtok(line, " ");
        do {
            query.point.data[idx++] = atof(token);
        } while ((token = strtok(NULL, " ")));

        query.k = k;
        queries.emplace_back(query);
    }

    free(line);
    fclose(qf);

    return queries;
}

static bool next_vector_record(std::fstream &file, Word2VecRec &record, bool binary=false) {
    std::string line;
    if (std::getline(file, line, '\n')) {
        std::stringstream line_stream(line);
        for (size_t i=0; i<300; i++) {
            std::string dimension;

            std::getline(line_stream, dimension, ' ');
            record.data[i] = atof(dimension.c_str());
        }

        g_reccnt++;

        return true;
    }

    return false;

}


template <de::KVPInterface R>
static bool next_record(std::fstream &file, R &record, bool binary=false)
{
    static value_type value = 1;
    if (g_reccnt >= g_max_record_cnt) return false;

    if (binary) {
        if (file.good()) {
            decltype(R::key) key; 

            file.read((char*) &key, sizeof(key));
            record.key = key;
            record.value = value;
            value++;

            if constexpr (de::WeightedRecordInterface<R>) {
                decltype(R::weight) weight;
                file.read((char*) &weight, sizeof(weight));
                record.weight = weight;
            }

            if (record.key < g_min_key) g_min_key = record.key;
            if (record.key > g_max_key) g_max_key = record.key;

            return true;
        }

        return false;
    }

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
                             double delete_prop, std::vector<R> &to_delete, bool binary=false) {
    vec.clear();
    for (size_t i=0; i<n; i++) {
        R rec;
        if constexpr (std::is_same_v<R, Word2VecRec>) {
            if (!next_vector_record(file, rec)) {
                if (i == 0) {
                    return false;
                }

                break;
            }
        } else {
            if (!next_record(file, rec, binary)) {
                if (i == 0) {
                    return false;
                }

                break;
            }
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
                   double delete_prop, std::vector<R> to_delete, bool progress=true, bool binary=false) {
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
        auto continue_warmup = build_insert_vec(file, insert_vec, batch, delete_prop, to_delete, binary);
        if (inserted > batch) {
            build_delete_vec(to_delete, delete_vec, batch*delete_prop);
            delete_idx = 0;
        }

        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (delete_idx < delete_vec.size() && gsl_rng_uniform(g_rng) < delete_prop) {
                if constexpr (std::is_same_v<TreeMap, DE>) {
                    extended_index.erase_one(delete_vec[delete_idx++].key);
                }
                else if constexpr (std::is_same_v<MTree, DE>) {
                    extended_index.remove(delete_vec[delete_idx++]);
                } else {
                    extended_index.erase(delete_vec[delete_idx++]);
                }
            }

            // insert the record;
            if constexpr (std::is_same_v<MTree, DE>) {
                extended_index.add(insert_vec[i]);
            } else {
                extended_index.insert(insert_vec[i]);
            }
            inserted++;

            if (progress) {
                progress_update((double) inserted / (double) count, "warming up:");
            }
        }
    }

    return true;
}


static void reset_de_perf_metrics() {

    /*
     * rejection counters are zeroed automatically by the
     * sampling function itself.
     */

    RESET_IO_CNT(); 
}
