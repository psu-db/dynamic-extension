#pragma once

#include <cstdlib>
#include "psu-ds/BTree.h"

struct btree_record {
    int64_t key;
    int64_t value;

   inline bool operator<(const btree_record& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }

    inline bool operator==(const btree_record& other) const {
        return key == other.key && value == other.value;
    }
};

struct btree_key_extract {
    static const int64_t &get(const btree_record &v) {
        return v.key;
    }
};

typedef psudb::BTree<int64_t, btree_record, btree_key_extract> BenchBTree;


