#pragma once

#include <cstdlib>
#include <vector>
#include <algorithm>
#include <limits>
#include <gsl/gsl_rng.h>

#include "psu-ds/PriorityQueue.h"
#include "framework/interface/Record.h"

template <typename R, size_t LEAFSZ=100>
class BSMVPTree {
public:
    struct KNNQueryParms {
        R point;
        size_t k;
    };

public:
    static BSMVPTree *build(std::vector<R> &records) {
        return new BSMVPTree(records);
    }

    static BSMVPTree *build_presorted(std::vector<R> &records) {
        return new BSMVPTree(records);
    }

    std::vector<R> unbuild() {
        return std::move(m_data);
    }

    std::vector<R> query(void *q) {
        std::vector<R> rs;

        /* return an empty result set if q is invalid */
        if (q == nullptr) {
            return rs;
        }

        auto parms = (BSMVPTree::KNNQueryParms*) q;
        auto pq = psudb::PriorityQueue<R, de::DistCmpMax<R>>(parms->k, &parms->point);

        if (parms->k >= m_data.size()) {
            for (size_t i=0; i<m_data.size(); i++) {
                if (m_ptrs[i].ptr != nullptr) {
                    pq.push(m_ptrs[i].ptr);
                }
            }
        } else {
            double farthest = std::numeric_limits<double>::max();
            internal_search(m_root, parms->point, parms->k, pq, &farthest);
        }

        size_t i=0;
        while (pq.size() > 0) {
           rs.push_back(*pq.peek().data);
           pq.pop();
        }
        return std::move(rs);
    }

    std::vector<R> query_merge(std::vector<R> &rsa, std::vector<R> &rsb, void* parms) {
        KNNQueryParms *p = (KNNQueryParms *) parms;
        R rec = p->point;
        size_t k = p->k;

        std::vector<R> output;

        psudb::PriorityQueue<R, de::DistCmpMax<R>> pq(k, &rec);

        for (size_t i=0; i<rsa.size(); i++) {
            if (pq.size() < k) {
                pq.push(&rsa[i]);
            } else {
                double head_dist = pq.peek().data->calc_distance(rec);
                double cur_dist = rsa[i].calc_distance(rec);

                if (cur_dist < head_dist) {
                    pq.pop();
                    pq.push(&rsa[i]);
                }
            }
        }

        for (size_t i=0; i<rsb.size(); i++) {
            if (pq.size() < k) {
                pq.push(&rsb[i]);
            } else {
                double head_dist = pq.peek().data->calc_distance(rec);
                double cur_dist = rsb[i].calc_distance(rec);

                if (cur_dist < head_dist) {
                    pq.pop();
                    pq.push(&rsb[i]);
                }
            }
        }

        while (pq.size() > 0) {
            output.emplace_back(*pq.peek().data);
            pq.pop();
        }

        return std::move(output);
    }

    size_t record_count() {
        return m_data.size();
    }

    ~BSMVPTree() {
        delete m_root;
    }

private:

    struct vp_ptr {
        R *ptr;
        double dist;
    };

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

    std::vector<R> m_data;
    std::vector<vp_ptr> m_ptrs;
    vpnode *m_root;

    size_t m_node_cnt;

    BSMVPTree(std::vector<R> &records) {
        m_data = std::move(records);
        m_node_cnt = 0;

        for (size_t i=0; i<m_data.size(); i++) {
            m_ptrs.push_back({&m_data[i], 0});
        }

        m_root = build_vptree();
    }

    vpnode *build_vptree() {
        if (m_data.size() == 0) {
            return nullptr;
        }

        size_t lower = 0;
        size_t upper = m_data.size() - 1;

        auto rng = gsl_rng_alloc(gsl_rng_mt19937);
        auto root = build_subtree(lower, upper, rng);
        gsl_rng_free(rng);
        return root;
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
            m_ptrs[i].dist = m_ptrs[start].ptr->calc_distance(*m_ptrs[i].ptr);
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
        node->radius = m_ptrs[start].ptr->calc_distance(*m_ptrs[mid].ptr);
        m_ptrs[start].dist = node->radius;

        /* recursively construct the left and right subtrees */
        node->inside = build_subtree(start + 1, mid-1, rng); 
        node->outside = build_subtree(mid, stop, rng);

        m_node_cnt++;

        return node;
    }

    void quickselect(size_t start, size_t stop, size_t k, R *p, gsl_rng *rng) {
        if (start == stop) return;

        auto pivot = partition(start, stop, p, rng);

        if (k < pivot) {
            quickselect(start, pivot - 1, k, p, rng);
        } else if (k > pivot) {
            quickselect(pivot + 1, stop, k, p, rng);
        }
    }

    size_t partition(size_t start, size_t stop, R *p, gsl_rng *rng) {
        auto pivot = start + gsl_rng_uniform_int(rng, stop - start);

        swap(pivot, stop);

        size_t j = start;
        for (size_t i=start; i<stop; i++) {
            if (m_ptrs[i].dist < m_ptrs[stop].dist)  {
                swap(j++, i);
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

     void internal_search(vpnode *node, const R &point, size_t k, psudb::PriorityQueue<R, 
                de::DistCmpMax<R>> &pq, double *farthest) {

        if (node == nullptr) return;

        if (node->leaf) {
            for (size_t i=node->start; i<=node->stop; i++) {
                double d = point.calc_distance(*m_ptrs[i].ptr);
                if (d < *farthest) {
                    if (pq.size() == k) {
                        pq.pop();
                    }

                    pq.push(m_ptrs[i].ptr);
                    if (pq.size() == k) {
                        *farthest = point.calc_distance(*pq.peek().data);
                    }
                }
            }

            return;
        }

        double d = point.calc_distance(*m_ptrs[node->start].ptr);

        if (d < *farthest) {
            if (pq.size() == k) {
                auto t = pq.peek().data;
                pq.pop();
            }
            pq.push(m_ptrs[node->start].ptr);
            if (pq.size() == k) {
                *farthest = point.calc_distance(*pq.peek().data);
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
