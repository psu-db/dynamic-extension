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

typedef uint32_t hdr_t;
typedef uint64_t key_t;
typedef uint32_t value_t;
typedef uint64_t weight_t;

struct Record {
    key_t key;
    value_t value;
    hdr_t header;
    weight_t weight;

    inline bool match(key_t k, value_t v, bool is_tombstone) const {
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

    inline bool lt(const key_t& k, const value_t& v) const {
        return key < k || (key == k && value < v);
    }
};

static_assert(sizeof(Record) == 24, "Record is not 24 bytes long.");

static bool memtable_record_cmp(const Record& a, const Record& b) {
    return (a.key < b.key) || (a.key == b.key && a.value < b.value)
        || (a.key == b.key && a.value == b.value && a.header < b.header);
}

}
