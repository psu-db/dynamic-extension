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

};


template <typename R>
class KNNDistCmpMax {
public:
    KNNDistCmpMax(R *baseline) : P(baseline) {}

    inline bool operator()(const R *a, const R *b) requires WrappedInterface<R> {
        return a->rec.calc_distance(P->rec) > b->rec.calc_distance(P->rec); 
    }

    inline bool operator()(const R *a, const R *b) requires (!WrappedInterface<R>){
        return a->calc_distance(*P) > b->calc_distance(*P); 
    }

private:
    R *P;
};

template <typename R>
class KNNDistCmpMin {
public:
    KNNDistCmpMin(R *baseline) : P(baseline) {}

    inline bool operator()(const R *a, const R *b) requires WrappedInterface<R> {
        return a->rec.calc_distance(P->rec) < b->rec.calc_distance(P->rec); 
    }

    inline bool operator()(const R *a, const R *b) requires (!WrappedInterface<R>){
        return a->calc_distance(*P) < b->calc_distance(*P); 
    }

private:
    R *P;
};



template <NDRecordInterface R, size_t LEAFSZ=100, bool HMAP=false>
class VPTree {
private:
    struct vpnode {
        size_t start;
        size_t stop;
        bool leaf;

        double radius;
        vpnode *inside;
        vpnode *outside;

        vpnode() : start(0), stop(0), leaf(false), radius(0.0), inside(nullptr), outside(nullptr) {}

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
        m_ptrs = new Wrapped<R>*[buffer->get_record_count()];

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
            m_data[m_reccnt] = *rec;
            m_ptrs[m_reccnt] = &m_data[m_reccnt];
            m_reccnt++;
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
        m_ptrs = new Wrapped<R>*[attemp_reccnt];

        // FIXME: will eventually need to figure out tombstones
        //        this one will likely require the multi-pass
        //        approach, as otherwise we'll need to sort the
        //        records repeatedly on each reconstruction.
        for (size_t i=0; i<len; i++) {
            for (size_t j=0; j<shards[i]->get_record_count(); j++) {
                if (shards[i]->get_record_at(j)->is_deleted()) {
                    continue;
                }

                m_data[m_reccnt] = *shards[i]->get_record_at(j);
                m_ptrs[m_reccnt] = &m_data[m_reccnt];
                m_reccnt++;
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
        if (m_ptrs) delete[] m_ptrs;
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter=false) {
        if constexpr (HMAP) {
            auto idx = m_lookup_map.find(rec);

            if (idx == m_lookup_map.end()) {
                return nullptr;
            }

            return m_data + idx->second;
        } else {
            vpnode *node = m_root;

            while (!node->leaf && m_ptrs[node->start]->rec != rec) {
                if (rec.calc_distance((m_ptrs[node->start]->rec)) >= node->radius) {
                    node = node->outside;
                } else {
                    node = node->inside;
                }
            }

            for (size_t i=node->start; i<=node->stop; i++) {
                if (m_ptrs[i]->rec == rec) {
                    return m_ptrs[i];
                }
            }

            return nullptr;
        }
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
        if (m_reccnt == 0) {
            return nullptr;
        }

        size_t lower = 0;
        size_t upper = m_reccnt - 1;

        auto rng = gsl_rng_alloc(gsl_rng_mt19937);
        auto root = build_subtree(lower, upper, rng);
        gsl_rng_free(rng);
        return root;
    }

    void build_map() {
        // Skip constructing the hashmap if disabled in the
        // template parameters.
        if constexpr (!HMAP) {
            return;
        }

        for (size_t i=0; i<m_reccnt; i++) {
          // FIXME: Will need to account for tombstones here too. Under
          // tombstones, it is technically possible for two otherwise identical
          // instances of the same record to exist within the same shard, so
          // long as one of them is a tombstone. Because the table is currently
          // using the unwrapped records for the key, it isn't possible for it
          // to handle this case right now.
          m_lookup_map.insert({m_data[i].rec, i});
        }
    }

    vpnode *build_subtree(size_t start, size_t stop, gsl_rng *rng) {
        // base-case: sometimes happens (probably because of the +1 and -1
        // in the first recursive call)
        if (start > stop) {
            return nullptr;
        }

        // base-case: create a leaf node
        if (stop - start <= LEAFSZ) {
            vpnode *node = new vpnode();
            node->start = start;
            node->stop = stop;
            node->leaf = true;

            m_node_cnt++;
            return node;
        }

        // select a random element to be the root of the
        // subtree
        auto i = start + gsl_rng_uniform_int(rng, stop - start + 1);
        swap(start, i);

        // partition elements based on their distance from the start,
        // with those elements with distance falling below the median
        // distance going into the left sub-array and those above 
        // the median in the right. This is easily done using QuickSelect.
        auto mid = (start + 1 + stop) / 2;
        quickselect(start + 1, stop, mid, m_ptrs[start], rng);

        // Create a new node based on this partitioning
        vpnode *node = new vpnode();
        node->start = start;

        // store the radius of the circle used for partitioning the node.
        node->radius = m_ptrs[start]->rec.calc_distance(m_ptrs[mid]->rec);

        // recursively construct the left and right subtrees
        node->inside = build_subtree(start + 1, mid-1, rng); 
        node->outside = build_subtree(mid, stop, rng);

        m_node_cnt++;

        return node;
    }


    void quickselect(size_t start, size_t stop, size_t k, Wrapped<R> *p, gsl_rng *rng) {
        if (start == stop) return;

        auto pivot = partition(start, stop, p, rng);

        if (k < pivot) {
            quickselect(start, pivot - 1, k, p, rng);
        } else if (k > pivot) {
            quickselect(pivot + 1, stop, k, p, rng);
        }
    }


    size_t partition(size_t start, size_t stop, Wrapped<R> *p, gsl_rng *rng) {
        auto pivot = start + gsl_rng_uniform_int(rng, stop - start);
        double pivot_dist = p->rec.calc_distance(m_ptrs[pivot]->rec);

        swap(pivot, stop);

        size_t j = start;
        for (size_t i=start; i<stop; i++) {
            if (p->rec.calc_distance(m_ptrs[i]->rec) < pivot_dist) {
                swap(j, i);
                j++;
            }
        }

        swap(j, stop);
        return j;
    }


    void swap(size_t idx1, size_t idx2) {
        auto tmp = m_ptrs[idx1];
        m_ptrs[idx1] = m_ptrs[idx2];
        m_ptrs[idx2] = tmp;
    }


    void search(vpnode *node, const R &point, size_t k, PriorityQueue<Wrapped<R>, KNNDistCmpMax<Wrapped<R>>> &pq, double *farthest) {
        if (node == nullptr) return;

        if (node->leaf) {
            for (size_t i=node->start; i<=node->stop; i++) {
                double d = point.calc_distance(m_ptrs[i]->rec);
                if (d < *farthest) {
                    if (pq.size() == k) {
                        pq.pop();
                    }

                    pq.push(m_ptrs[i]);
                    if (pq.size() == k) {
                        *farthest = point.calc_distance(pq.peek().data->rec);
                    }
                }
            }

            return;
        }

        double d = point.calc_distance(m_ptrs[node->start]->rec);

        if (d < *farthest) {
            if (pq.size() == k) {
                auto t = pq.peek().data->rec;
                pq.pop();
            }
            pq.push(m_ptrs[node->start]);
            if (pq.size() == k) {
                *farthest = point.calc_distance(pq.peek().data->rec);
            }
        }

        if (d < node->radius) {
            if (d - (*farthest) <= node->radius) {
                search(node->inside, point, k, pq, farthest);
            }

            if (d + (*farthest) >= node->radius) {
                search(node->outside, point, k, pq, farthest);
            }
        } else {
            if (d + (*farthest) >= node->radius) {
                search(node->outside, point, k, pq, farthest);
            }

            if (d - (*farthest) <= node->radius) {
                search(node->inside, point, k, pq, farthest);
            }
        }
    }

    Wrapped<R>* m_data;
    Wrapped<R>** m_ptrs;
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
        return;
    }

    static std::vector<Wrapped<R>> query(VPTree<R> *wss, void *q_state, void *parms) {
        std::vector<Wrapped<R>> results;
        KNNQueryParms<R> *p = (KNNQueryParms<R> *) parms;
        Wrapped<R> wrec;
        wrec.rec = p->point;
        wrec.header = 0;

        PriorityQueue<Wrapped<R>, KNNDistCmpMax<Wrapped<R>>> pq(p->k, &wrec);

        double farthest = std::numeric_limits<double>::max();

        wss->search(wss->m_root, p->point, p->k, pq, &farthest);

        while (pq.size() > 0) {
            results.emplace_back(*pq.peek().data);
            pq.pop();
        }

        return results;
    }

    static std::vector<Wrapped<R>> buffer_query(MutableBuffer<R> *buffer, void *state, void *parms) {
        KNNQueryParms<R> *p = (KNNQueryParms<R> *) parms;
        Wrapped<R> wrec;
        wrec.rec = p->point;
        wrec.header = 0;

        size_t k = p->k;

        PriorityQueue<Wrapped<R>, KNNDistCmpMax<Wrapped<R>>> pq(k, &wrec);
        for (size_t i=0; i<buffer->get_record_count(); i++) {
            // Skip over deleted records (under tagging)
            if ((buffer->get_data())[i].is_deleted()) {
                continue;
            }

            if (pq.size() < k) {
                pq.push(buffer->get_data() + i);
            } else {
                double head_dist = pq.peek().data->rec.calc_distance(wrec.rec);
                double cur_dist = (buffer->get_data() + i)->rec.calc_distance(wrec.rec);

                if (cur_dist < head_dist) {
                    pq.pop();
                    pq.push(buffer->get_data() + i);
                }
            }
        }

        std::vector<Wrapped<R>> results;
        while (pq.size() > 0) {
            results.emplace_back(*(pq.peek().data));
            pq.pop();
        }

        return results;
    }

    static std::vector<R> merge(std::vector<std::vector<R>> &results, void *parms) {
        KNNQueryParms<R> *p = (KNNQueryParms<R> *) parms;
        R rec = p->point;
        size_t k = p->k;

        PriorityQueue<R, KNNDistCmpMax<R>> pq(k, &rec);
        for (size_t i=0; i<results.size(); i++) {
            for (size_t j=0; j<results[i].size(); j++) {
                if (pq.size() < k) {
                    pq.push(&results[i][j]);
                } else {
                    double head_dist = pq.peek().data->calc_distance(rec);
                    double cur_dist = results[i][j].calc_distance(rec);

                    if (cur_dist < head_dist) {
                        pq.pop();
                        pq.push(&results[i][j]);
                    }
                }
            }
        }

        std::vector<R> output;
        while (pq.size() > 0) {
            output.emplace_back(*pq.peek().data);
            pq.pop();
        }

        return output;
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
