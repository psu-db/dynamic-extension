/*
 * include/util/record.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu>
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <cstring>

#include "util/base.h"

namespace de {

template <typename K, typename V, typename W=void>
struct Record {
    K key;
    V value;
    typename std::conditional<!std::is_same<W, void>::value, W, std::false_type>::type weight;
    uint32_t header;

    inline bool match(K k, V v, bool is_tombstone) const {
        return (key == k) && (value == v) && ((header & 1) == is_tombstone);
    }

    inline void set_delete_status() {
        header |= 2;
    }

    inline bool get_delete_status() const {
        return header & 2;
    }

    inline bool is_tombstone() const {
        return header & 1;
    }

    inline int match(const Record* other) const {
        return key == other->key && value == other->value;
    }

    inline bool operator<(const Record& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }

    inline bool lt(const K& k, const V& v) const {
        return key < k || (key == k && value < v);
    }
};


template <typename K, typename V, typename W=void>
static bool memtable_record_cmp(const Record<K, V, W>& a, const Record<K, V, W>& b) {
    return (a.key < b.key) || (a.key == b.key && a.value < b.value)
        || (a.key == b.key && a.value == b.value && a.header < b.header);
}

}
