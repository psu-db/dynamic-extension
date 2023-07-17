/*
 * include/shard/VPTree.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu>
 *
 * All outsides reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <cassert>
#include <queue>
#include <memory>
#include <concepts>
#include <map>

#include "ds/PriorityQueue.h"
#include "util/Cursor.h"
#include "ds/BloomFilter.h"
#include "util/bf_config.h"
#include "framework/MutableBuffer.h"
#include "framework/RecordInterface.h"
#include "framework/ShardInterface.h"
#include "framework/QueryInterface.h"

namespace de {

thread_local size_t wss_cancelations = 0;

template <NDRecordInterface R>
struct KNNQueryParms {
    R point;
    size_t k;
};

template <NDRecordInterface R>
class KNNQuery;

template <NDRecordInterface R>
struct KNNState {
    size_t k;

    KNNState() {
        k = 0;
    }
};

template <NDRecordInterface R>
struct KNNBufferState {
    size_t cutoff;
    size_t sample_size;
    Alias* alias;
    decltype(R::weight) max_weight;
    decltype(R::weight) total_weight;

    ~KNNBufferState() {
        delete alias;
    }

};

template <NDRecordInterface R>
class VPTree {
private:
    struct vpnode {
        size_t idx = 0;
        double radius = 0;
        vpnode *inside = nullptr;
        vpnode *outside = nullptr;

        ~vpnode() {
            delete inside;
            delete outside;
        }
    };

public:
    friend class KNNQuery<R>;

    VPTree(MutableBuffer<R>* buffer)
    : m_reccnt(0), m_tombstone_cnt(0), m_root(nullptr), m_node_cnt(0) {

        size_t alloc_size = (buffer->get_record_count() * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (buffer->get_record_count() * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        size_t offset = 0;
        m_reccnt = 0;

        // FIXME: will eventually need to figure out tombstones
        //        this one will likely require the multi-pass
        //        approach, as otherwise we'll need to sort the
        //        records repeatedly on each reconstruction.
        for (size_t i=0; i<buffer->get_record_count(); i++) {
            auto rec = buffer->get_data() + i;

            if (rec->is_deleted()) {
                continue;
            }

            rec->header &= 3;
            m_data[m_reccnt++] = *rec;
        }

        if (m_reccnt > 0) {
            m_root = build_vptree();
            build_map();
        }
    }

    VPTree(VPTree** shards, size_t len)
    : m_reccnt(0), m_tombstone_cnt(0), m_root(nullptr), m_node_cnt(0) {

        size_t attemp_reccnt = 0;

        for (size_t i=0; i<len; i++) {
            attemp_reccnt += shards[i]->get_record_count();
        }
        
        size_t alloc_size = (attemp_reccnt * sizeof(Wrapped<R>)) + (CACHELINE_SIZE - (attemp_reccnt * sizeof(Wrapped<R>)) % CACHELINE_SIZE);
        assert(alloc_size % CACHELINE_SIZE == 0);
        m_data = (Wrapped<R>*)std::aligned_alloc(CACHELINE_SIZE, alloc_size);

        // FIXME: will eventually need to figure out tombstones
        //        this one will likely require the multi-pass
        //        approach, as otherwise we'll need to sort the
        //        records repeatedly on each reconstruction.
        for (size_t i=0; i<len; i++) {
            for (size_t j=0; j<shards[i]->get_record_count(); j++) {
                if (shards[i]->get_record_at(j)->is_deleted()) {
                    continue;
                }

                m_data[m_reccnt++] = *shards[i]->get_record_at(j);
            }
        }

        if (m_reccnt > 0) {
            m_root = build_vptree();
            build_map();
        }
   }

    ~VPTree() {
        if (m_data) free(m_data);
        if (m_root) delete m_root;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        auto idx = m_lookup_map.find(rec);

        if (idx == m_lookup_map.end()) {
            return nullptr;
        }

        return m_data + idx->second;
    }

    Wrapped<R>* get_data() const {
        return m_data;
    }
    
    size_t get_record_count() const {
        return m_reccnt;
    }

    size_t get_tombstone_count() const {
        return m_tombstone_cnt;
    }

    const Wrapped<R>* get_record_at(size_t idx) const {
        if (idx >= m_reccnt) return nullptr;
        return m_data + idx;
    }


    size_t get_memory_usage() {
        return m_node_cnt * sizeof(vpnode);
    }

private:

    vpnode *build_vptree() {
        assert(m_reccnt > 0);

        size_t lower = 0;
        size_t upper = m_reccnt - 1;

        auto rng = gsl_rng_alloc(gsl_rng_mt19937);
        auto n = build_subtree(lower, upper, rng);
        gsl_rng_free(rng);
        return n;
    }

    void build_map() {

        for (size_t i=0; i<m_reccnt; i++) {
            m_lookup_map.insert({m_data[i].rec, i});
        }
    }

    vpnode *build_subtree(size_t start, size_t stop, gsl_rng *rng) {
        if (start >= stop) {
            return nullptr;
        }

        // select a random element to partition based on, and swap
        // it to the front of the sub-array
        auto i = start + gsl_rng_uniform_int(rng, stop - start);
        swap(start, i);

        // partition elements based on their distance from the start,
        // with those elements with distance falling below the median
        // distance going into the left sub-array and those above 
        // the median in the right. This is easily done using QuickSelect.
        auto mid = ((start+1) + stop) / 2;
        quickselect(start + 1, stop, mid, m_data[start], rng);

        // Create a new node based on this partitioning
        vpnode *node = new vpnode();

        // store the radius of the circle used for partitioning the
        // node.
        node->idx = start;
        node->radius = m_data[start].rec.calc_distance(m_data[mid].rec);

        // recursively construct the left and right subtrees
        node->inside = build_subtree(start + 1, mid - 1, rng); 
        node->outside = build_subtree(mid, stop, rng);

        m_node_cnt++;

        return node;
    }


    void quickselect(size_t start, size_t stop, size_t k, Wrapped<R> p, gsl_rng *rng) {
        if (start == stop) return;

        auto pivot = partition(start, stop, p, rng);

        if (k < pivot) {
            quickselect(start, pivot - 1, k, p, rng);
        } else if (k > pivot) {
            quickselect(pivot + 1, stop, k, p, rng);
        }
    }


    size_t partition(size_t start, size_t stop, Wrapped<R> p, gsl_rng *rng) {
        auto pivot = start + gsl_rng_uniform_int(rng, stop - start);
        double pivot_dist = p.rec.calc_distance(m_data[pivot].rec);

        swap(pivot, stop);

        size_t j = start;
        for (size_t i=start; i<stop; i++) {
            if (p.rec.calc_distance(m_data[i].rec) < pivot_dist) {
                swap(j, i);
                j++;
            }
        }

        swap(j, stop);
        return j;
    }


    void swap(size_t idx1, size_t idx2) {
        Wrapped<R> tmp = m_data[idx1];
        m_data[idx1] = m_data[idx2];
        m_data[idx2] = tmp;
    }

    Wrapped<R>* m_data;
    std::unordered_map<R, size_t, RecordHash<R>> m_lookup_map;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_node_cnt;

    vpnode *m_root;

};


template <NDRecordInterface R>
class KNNQuery {
public:
    static void *get_query_state(VPTree<R> *wss, void *parms) {
        return nullptr;
    }

    static void* get_buffer_query_state(MutableBuffer<R> *buffer, void *parms) {
        return nullptr;
    }

    static void process_query_states(void *query_parms, std::vector<void*> shard_states, void *buff_state) {
    }

    static std::vector<Wrapped<R>> query(VPTree<R> *wss, void *q_state, void *parms) {
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results) {
    }

    static void delete_query_state(void *state) {
        auto s = (KNNState<R> *) state;
        delete s;
    }

    static void delete_buffer_query_state(void *state) {
        auto s = (KNNBufferState<R> *) state;
        delete s;
    }
};

}
