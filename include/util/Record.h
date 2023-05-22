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
#include <concepts>

#include "util/base.h"

namespace de {

template<typename R>
concept RecordInterface = requires(R r, R s) {
    r.key;
    r.value;

    { r < s } ->std::convertible_to<bool>;
    { r == s } ->std::convertible_to<bool>;
};

template<RecordInterface R>
struct WrappedRecord {
    R rec;
    uint32_t header;

    inline void set_delete() {
        header |= 2;
    }

    inline bool is_deleted() const {
        return header & 2;
    }

    inline void set_tombstone(bool val=true) {
        if (val) {
            header |= val;
        } else {
            header &= 0;
        }
    }

    inline bool is_tombstone() const {
        return header & 1;
    }
};

template <typename R>
concept WeightedRecordInterface = RecordInterface<R> && requires(R r) {
    {r.weight} -> std::convertible_to<double>;
};

template <typename K, typename V>
struct Record {
    K key;
    V value;
    uint32_t header = 0;

       inline bool operator<(const Record& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }

    inline bool operator==(const Record& other) const {
        return key == other.key && value == other.value;
    }
};

template <typename K, typename V, typename W>
struct WeightedRecord {
    K key;
    V value;
    W weight = 1;
    uint32_t header = 0;

    inline void set_delete() {
        header |= 2;
    }

    inline bool is_deleted() const {
        return header & 2;
    }

    inline void set_tombstone(bool val=true) {
        if (val) {
            header |= val;
        } else {
            header &= 0;
        }
    }

    inline bool is_tombstone() const {
        return header & 1;
    }

    inline int match(const WeightedRecord* other) const {
        return key == other->key && value == other->value;
    }

    inline bool operator<(const WeightedRecord& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }

    inline bool operator==(const WeightedRecord& other) const {
        return key == other.key && value == other.value;
    }
};


template <RecordInterface R>
static bool memtable_record_cmp(const R& a, const R& b) {
    return (a.key < b.key) || (a.key == b.key && a.value < b.value)
        || (a.key == b.key && a.value == b.value && a.header < b.header);
}

}
