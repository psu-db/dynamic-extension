#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <gsl/gsl_rng.h>
#include <cstring>
#include <vector>

#include "framework/interface/Record.h"
#include "query/irs.h"

#pragma once

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

template<de::KVPInterface R>
static std::vector<R> read_sosd_file(std::string &fname, size_t n) {
    std::fstream file;
    file.open(fname, std::ios::in | std::ios::binary);

    std::vector<R> records(n);
    for (size_t i=0; i<n; i++) {
        uint64_t k;
        file.read((char*) &(k), sizeof(uint64_t));
        records[i].key = k;
        records[i].value = i;
    }

    return records;
}

/*
 * This function expects a plaintext file with each vector on its own line.
 * There should be D dimensions (or more) for each record, separated by
 * whitespace. The function will generate a vector of n records, each
 * record built from the first D dimensions of the data in the file.
 */
template <typename R, size_t D>
static std::vector<R> read_vector_file(std::string &fname, size_t n) {
    std::fstream file;
    file.open(fname, std::ios::in);

    std::vector<R> records;
    records.reserve(n);

    for (size_t i=0; i<n; i++) {
        std::string line;
        if (!std::getline(file, line, '\n')) break;

        std::stringstream line_stream(line);
        R rec;
        for (size_t j=0; j<D; j++) {
            std::string dim;
            if (!std::getline(line_stream, dim, ' ')) break;

            rec.data[j] = atof(dim.c_str());
        }
        records.emplace_back(rec);
    }

    return records;
}
