/*
 * include/ds/BloomFilter.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <vector>
#include <cassert>

#include "util/Record.h"

namespace de {

template <typename R>
struct queue_record {
    const R *data;
    size_t version;
};


template <typename R>
class PriorityQueue {
public:
    PriorityQueue(size_t size) : data(size), tail(0) {}
    ~PriorityQueue() = default;

    size_t size() const {
        return tail;
    }

    void pop() {
        assert(this->tail != 0);

        // If there is only one element, just decrement the
        // tail.
        if (this->size() == 1) {
            this->tail--;
            return;
        }

        swap(0, --this->tail);

        ssize_t idx = 0;
        ssize_t child_idx;

        while ((child_idx = min_child(idx)) != -1 && heap_cmp(child_idx, idx)) {
            swap(idx, child_idx);
            idx = child_idx;
        }
    }

    void push(const R* record, size_t version=0) {
        assert(tail != this->data.size());

        size_t new_idx = this->tail++;
        this->data[new_idx] = {record, version};

        while (new_idx != 0 && !heap_cmp(parent(new_idx), new_idx)) {
            swap(parent(new_idx), new_idx);
            new_idx = parent(new_idx);
        }
    }


    queue_record<R> peek(size_t depth=0) {
        ssize_t idx = 0;
        size_t cur_depth = 0;

        while (cur_depth != depth) {
            idx = min_child(idx);
            assert(idx != -1);
            cur_depth++;
        }

        return this->data[idx];
    }

private:
    std::vector<queue_record<R>> data;
    size_t tail;

    /*
     * Swap the elements at position a and position
     * b within the heap
     */
    inline void swap(size_t a, size_t b) {
        if (a == b) return;

        queue_record temp = this->data[a];
        this->data[a] = this->data[b];
        this->data[b] = temp;
    }

    inline size_t left_child(size_t idx) {
        return 2 * idx + 1;
    }

    inline size_t right_child(size_t idx) {
        return 2 * idx + 2;
    }

    inline size_t parent(size_t idx) {
        return (idx - 1) / 2;
    }

    inline ssize_t min_child(size_t idx) {
        ssize_t left = left_child(idx) < tail ? left_child(idx) : -1;
        ssize_t right = right_child(idx) < tail ? right_child(idx) : -1;

        if (left == -1 && right == -1) {
            return -1;
        } else if (left == -1) {
            return right;
        } else  if (right == -1) {
            return left;
        }

        return (heap_cmp(left, right)) ? left : right;
    }

    inline bool heap_cmp(size_t a, size_t b) {
        if (data[a].data != data[b].data) {
            return *(data[a].data) < *(data[b].data);
        } else if (data[a].version != data[b].version)
            return data[a].version < data[b].version;
        else return data[a].data->is_tombstone() && data[b].data->is_tombstone();
    }

};
}
