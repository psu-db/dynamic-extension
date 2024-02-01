#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <gsl/gsl_rng.h>
#include <cstring>
#include <vector>

#include "psu-ds/BTree.h"

#pragma once

typedef uint64_t key_type;
typedef uint64_t value_type;
typedef uint64_t weight_type;

static gsl_rng *g_rng;
static bool g_osm_data;

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

typedef psudb::BTree<int64_t, btree_record, btree_key_extract> BenchBTree;

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
    g_osm_data = osm_correction;
    g_max_record_cnt = max_reccnt;
    g_reccnt = 0;
}

static void delete_bench_env()
{
    gsl_rng_free(g_rng);
}


template <typename QP>
static std::vector<QP> read_lookup_queries(std::string fname, double selectivity) {
    std::vector<QP> queries;

    FILE *qf = fopen(fname.c_str(), "r");
    size_t start, stop;
    double sel;
    while (fscanf(qf, "%zu%zu%lf\n", &start, &stop, &sel) != EOF) {
        if (start < stop && std::abs(sel - selectivity) < 0.1) {
            QP q;
            q.target_key = start;
            queries.push_back(q);
        }
    }
    fclose(qf);

    return queries;
}

template <typename QP>
static std::vector<QP> read_range_queries(std::string &fname, double selectivity) {
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

/*
 * NOTE: The QP type must have lower_bound and upper_bound attributes, which
 * this function will initialize. Any other query parameter attributes must
 * be manually initialized after the call.
 */
template <typename R>
static bool next_vector_record(std::fstream &file, R &record, bool binary=false) {
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

template <typename R>
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

        if (record.key < g_min_key) g_min_key = record.key;
        if (record.key > g_max_key) g_max_key = record.key;

        g_reccnt++;

        return true;
    }

    return false;
}

template <typename R>
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

static std::vector<int64_t> read_sosd_file(std::string &fname, size_t n) {
    std::fstream file;
    file.open(fname, std::ios::in | std::ios::binary);

    std::vector<int64_t> records(n);
    for (size_t i=0; i<n; i++) {
        file.read((char*) &(records[i]), sizeof(int64_t));
    }

    return records;
}
