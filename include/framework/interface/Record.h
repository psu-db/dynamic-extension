/*
 * include/framework/interface/Record.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * FIXME: the record implementations could probably be broken out into 
 *        different files, leaving only the interface here
 */
#pragma once

#include <cstring>
#include <concepts>
#include <cmath>

#include "psu-util/hash.h"

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

template<typename R>
concept AlexInterface = KVPInterface<R> && requires(R r) {
    {r.key} -> std::convertible_to<size_t>;
    {r.value} -> std::convertible_to<size_t>;
};

template<typename R>
concept WrappedInterface = RecordInterface<R> && requires(R r, R s, bool b) {
    {r.header} -> std::convertible_to<uint32_t>;
     r.rec;
    {r.set_delete()};
    {r.is_deleted()} -> std::convertible_to<bool>;
    {r.set_tombstone(b)};
    {r.is_tombstone()} -> std::convertible_to<bool>;
    {r < s} -> std::convertible_to<bool>;
    {r == s} ->std::convertible_to<bool>;
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

    inline bool operator==(const Wrapped& other) const {
        return rec == other.rec;
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


template <typename V, size_t D=2>
struct CosinePoint{
    V data[D];

    inline bool operator==(const CosinePoint& other) const {
        for (size_t i=0; i<D; i++) {
            if (data[i] != other.data[i]) {
                return false;
            }
        }

        return true;
    }

    // lexicographic order
    inline bool operator<(const CosinePoint& other) const {
        for (size_t i=0; i<D; i++) {
            if (data[i] < other.data[i]) {
                return true;
            } else if (data[i] > other.data[i]) {
                return false;
            }
        }

        return false;
    }

    inline double calc_distance(const CosinePoint& other) const {

        double prod = 0;
        double asquared = 0;
        double bsquared = 0;

        for (size_t i=0; i<D; i++) {
            prod += data[i] * other.data[i];
            asquared += data[i]*data[i];
            bsquared += other.data[i]*other.data[i];
        }

        return prod / std::sqrt(asquared * bsquared);
    }
};


template <typename V, size_t D=2>
struct EuclidPoint{
    V data[D];

    inline bool operator==(const EuclidPoint& other) const {
        for (size_t i=0; i<D; i++) {
            if (data[i] != other.data[i]) {
                return false;
            }
        }

        return true;
    }

    // lexicographic order
    inline bool operator<(const EuclidPoint& other) const {
        for (size_t i=0; i<D; i++) {
            if (data[i] < other.data[i]) {
                return true;
            } else if (data[i] > other.data[i]) {
                return false;
            }
        }

        return false;
    }

    inline double calc_distance(const EuclidPoint& other) const {
        double dist = 0;
        for (size_t i=0; i<D; i++) {
            dist += (data[i] - other.data[i]) * (data[i] - other.data[i]);
        }
        
        return std::sqrt(dist);
    }
};

template<RecordInterface R>
struct RecordHash {
    size_t operator()(R const &rec) const {
        return psudb::hash_bytes((std::byte *) &rec, sizeof(R));
    }
};

}
