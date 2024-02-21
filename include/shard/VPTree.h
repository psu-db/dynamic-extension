/*
 * include/shard/VPTree.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around a VPTree for high-dimensional metric similarity
 * search.
 *
 * FIXME: Does not yet support the tombstone delete policy.
 * TODO: The code in this file is very poorly commented.
 */
#pragma once

#include <vector>

#include <unordered_map>
#include "framework/ShardRequirements.h"
#include "psu-ds/PriorityQueue.h"

using psudb::CACHELINE_SIZE;
using psudb::PriorityQueue;
using psudb::queue_record;
using psudb::byte;

namespace de {

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
    VPTree(BufferView<R> buffer)
    : m_reccnt(0), m_tombstone_cnt(0), m_root(nullptr), m_node_cnt(0) {


        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               buffer.get_record_count() * 
                                                 sizeof(Wrapped<R>), 
                                               (byte**) &m_data);

        m_ptrs = new vp_ptr[buffer.get_record_count()];

        size_t offset = 0;
        m_reccnt = 0;

        // FIXME: will eventually need to figure out tombstones
        //        this one will likely require the multi-pass
        //        approach, as otherwise we'll need to sort the
        //        records repeatedly on each reconstruction.
        for (size_t i=0; i<buffer.get_record_count(); i++) {
            auto rec = buffer.get(i);

            if (rec->is_deleted()) {
                continue;
            }

            rec->header &= 3;
            m_data[m_reccnt] = *rec;
            m_ptrs[m_reccnt].ptr = &m_data[m_reccnt];
            m_reccnt++;
        }

        if (m_reccnt > 0) {
            m_root = build_vptree();
            build_map();
        }
    }

    VPTree(std::vector<VPTree*> shards) 
    : m_reccnt(0), m_tombstone_cnt(0), m_root(nullptr), m_node_cnt(0) {

        size_t attemp_reccnt = 0;
        for (size_t i=0; i<shards.size(); i++) {
            attemp_reccnt += shards[i]->get_record_count();
        }

        m_alloc_size = psudb::sf_aligned_alloc(CACHELINE_SIZE, 
                                               attemp_reccnt * sizeof(Wrapped<R>),
                                               (byte **) &m_data);
        m_ptrs = new vp_ptr[attemp_reccnt];

        // FIXME: will eventually need to figure out tombstones
        //        this one will likely require the multi-pass
        //        approach, as otherwise we'll need to sort the
        //        records repeatedly on each reconstruction.
        for (size_t i=0; i<shards.size(); i++) {
            for (size_t j=0; j<shards[i]->get_record_count(); j++) {
                if (shards[i]->get_record_at(j)->is_deleted()) {
                    continue;
                }

                m_data[m_reccnt] = *shards[i]->get_record_at(j);
                m_ptrs[m_reccnt].ptr = &m_data[m_reccnt];
                m_reccnt++;
            }
        }

        if (m_reccnt > 0) {
            m_root = build_vptree();
            build_map();
        }
   }

    ~VPTree() {
        free(m_data);
        delete m_root;
        delete[] m_ptrs;
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

            while (!node->leaf && m_ptrs[node->start].ptr->rec != rec) {
                if (rec.calc_distance((m_ptrs[node->start].ptr->rec)) >= node->radius) {
                    node = node->outside;
                } else {
                    node = node->inside;
                }
            }

            for (size_t i=node->start; i<=node->stop; i++) {
                if (m_ptrs[i].ptr->rec == rec) {
                    return m_ptrs[i].ptr;
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
        return m_node_cnt * sizeof(vpnode) + m_reccnt * sizeof(R*) + m_alloc_size;
    }

    size_t get_aux_memory_usage() {
        // FIXME: need to return the size of the unordered_map
        return 0;
    }

    void search(const R &point, size_t k, PriorityQueue<Wrapped<R>, 
                DistCmpMax<Wrapped<R>>> &pq) {
        double farthest = std::numeric_limits<double>::max();
        
        internal_search(m_root, point, k, pq, &farthest);
    }

private:
    struct vp_ptr {
        Wrapped<R> *ptr;
        double dist;
    };
    Wrapped<R>* m_data;
    vp_ptr* m_ptrs;
    std::unordered_map<R, size_t, RecordHash<R>> m_lookup_map;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_node_cnt;
    size_t m_alloc_size;

    vpnode *m_root;

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
        /* 
         * base-case: sometimes happens (probably because of the +1 and -1
         * in the first recursive call)
         */
        if (start > stop) {
            return nullptr;
        }

        /* base-case: create a leaf node */
        if (stop - start <= LEAFSZ) {
            vpnode *node = new vpnode();
            node->start = start;
            node->stop = stop;
            node->leaf = true;

            m_node_cnt++;
            return node;
        }

        /* 
         * select a random element to be the root of the
         * subtree
         */
        auto i = start + gsl_rng_uniform_int(rng, stop - start + 1);
        swap(start, i);

        /* for efficiency, we'll pre-calculate the distances between each point and the root */
        for (size_t i=start+1; i<=stop; i++) {
            m_ptrs[i].dist = m_ptrs[start].ptr->rec.calc_distance(m_ptrs[i].ptr->rec);
        }

        /* 
         * partition elements based on their distance from the start,
         * with those elements with distance falling below the median
         * distance going into the left sub-array and those above 
         * the median in the right. This is easily done using QuickSelect.
         */
        auto mid = (start + 1 + stop) / 2;
        quickselect(start + 1, stop, mid, m_ptrs[start].ptr, rng);

        /* Create a new node based on this partitioning */
        vpnode *node = new vpnode();
        node->start = start;

        /* store the radius of the circle used for partitioning the node. */
        node->radius = m_ptrs[start].ptr->rec.calc_distance(m_ptrs[mid].ptr->rec);
        m_ptrs[start].dist = node->radius;

        /* recursively construct the left and right subtrees */
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

    // TODO: The quickselect code can probably be generalized and moved out
    //       to psudb-common instead.
    size_t partition(size_t start, size_t stop, Wrapped<R> *p, gsl_rng *rng) {
        auto pivot = start + gsl_rng_uniform_int(rng, stop - start);
        //double pivot_dist = p->rec.calc_distance(m_ptrs[pivot]->rec);

        swap(pivot, stop);

        size_t j = start;
        for (size_t i=start; i<stop; i++) {
            if (m_ptrs[i].dist < m_ptrs[stop].dist)  {
            //assert(distances[i - start] == p->rec.calc_distance(m_ptrs[i]->rec));
            //if (distances[i - start] < distances[stop - start]) {
            //if (p->rec .calc_distance(m_ptrs[i]->rec) < pivot_dist) {
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

    void internal_search(vpnode *node, const R &point, size_t k, PriorityQueue<Wrapped<R>, 
                DistCmpMax<Wrapped<R>>> &pq, double *farthest) {

        if (node == nullptr) return;

        if (node->leaf) {
            for (size_t i=node->start; i<=node->stop; i++) {
                double d = point.calc_distance(m_ptrs[i].ptr->rec);
                if (d < *farthest) {
                    if (pq.size() == k) {
                        pq.pop();
                    }

                    pq.push(m_ptrs[i].ptr);
                    if (pq.size() == k) {
                        *farthest = point.calc_distance(pq.peek().data->rec);
                    }
                }
            }

            return;
        }

        double d = point.calc_distance(m_ptrs[node->start].ptr->rec);

        if (d < *farthest) {
            if (pq.size() == k) {
                auto t = pq.peek().data->rec;
                pq.pop();
            }
            pq.push(m_ptrs[node->start].ptr);
            if (pq.size() == k) {
                *farthest = point.calc_distance(pq.peek().data->rec);
            }
        }

        if (d < node->radius) {
            if (d - (*farthest) <= node->radius) {
                internal_search(node->inside, point, k, pq, farthest);
            }

            if (d + (*farthest) >= node->radius) {
                internal_search(node->outside, point, k, pq, farthest);
            }
        } else {
            if (d + (*farthest) >= node->radius) {
                internal_search(node->outside, point, k, pq, farthest);
            }

            if (d - (*farthest) <= node->radius) {
                internal_search(node->inside, point, k, pq, farthest);
            }
        }
    }
   };
}
