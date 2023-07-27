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
#include "shard/Alex.h"
#include "mtree.h"
#include "standalone_utility.h"

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
typedef de::EuclidPoint<double, W2V_SIZE> Word2VecRec;

typedef de::DynamicExtension<WRec, de::WSS<WRec>, de::WSSQuery<WRec>> ExtendedWSS;
typedef de::DynamicExtension<Rec, de::TrieSpline<Rec>, de::TrieSplineRangeQuery<Rec>> ExtendedTSRQ;
typedef de::DynamicExtension<Rec, de::PGM<Rec>, de::PGMRangeQuery<Rec>, de::LayoutPolicy::TEIRING, de::DeletePolicy::TOMBSTONE> ExtendedPGMRQ;
typedef de::DynamicExtension<Rec, de::MemISAM<Rec>, de::IRSQuery<Rec>> ExtendedISAM_IRS;
typedef de::DynamicExtension<Rec, de::MemISAM<Rec>, de::ISAMRangeQuery<Rec>> ExtendedISAM_RQ;
typedef de::DynamicExtension<Word2VecRec, de::VPTree<Word2VecRec>, de::KNNQuery<Word2VecRec>> ExtendedVPTree_KNN;
//typedef de::DynamicExtension<Rec, de::Alex<Rec>, de::AlexRangeQuery<Rec>> ExtendedAlexRQ;

struct euclidean_distance {
    double operator()(const Word2VecRec &first, const Word2VecRec &second) const {
        double dist = 0;
        for (size_t i=0; i<W2V_SIZE; i++) {
            dist += (first.data[i] - second.data[i]) * (first.data[i] - second.data[i]);
        }
        
        return std::sqrt(dist);
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
typedef mt::mtree<Word2VecRec, euclidean_distance> MTree;

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
