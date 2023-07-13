/*
 * include/framework/RecordInterface.h
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
#include <cmath>

#include "util/base.h"

namespace de {

template<typename R>
concept RecordInterface = requires(R r, R s) {
    { r < s } ->std::convertible_to<bool>;
    { r == s } ->std::convertible_to<bool>;
};

template<typename R>
concept WeightedRecordInterface = requires(R r) {
    {r.weight} -> std::convertible_to<double>;
};

template<typename R>
concept NDRecordInterface = RecordInterface<R> && requires(R r, R s) {
    {r.calc_distance(s)} -> std::convertible_to<double>;
};

template <typename R>
concept KVPInterface = RecordInterface<R> && requires(R r) {
    r.key;
    r.value;
};

template<RecordInterface R>
struct Wrapped {
    uint32_t header;
    R rec;

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

    inline bool operator<(const Wrapped& other) const {
        return rec < other.rec || (rec == other.rec && header < other.header);
    }
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

    inline bool operator==(const WeightedRecord& other) const {
        return key == other.key && value == other.value;
    }

   inline bool operator<(const WeightedRecord& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }
};

template <typename V>
struct Point{
    V x;
    V y;

    inline bool operator==(const Point& other) const {
        return x == other.x && y == other.y;
    }

    // lexicographic order
    inline bool operator<(const Point& other) const {
        return x < other.x || (x == other.x && y < other.y);
    }

    inline double calc_distance(const Point& other) const {
        return sqrt(pow(x - other.x, 2) + pow(y - other.y, 2));
    }
};
}
