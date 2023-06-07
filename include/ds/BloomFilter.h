/*
 * include/ds/BloomFilter.h
 *
 * Copyright (C) 2023 Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <cmath>
#include <gsl/gsl_rng.h>

#include "ds/BitArray.h"
#include "util/hash.h"

namespace de {

template <typename K>
class BloomFilter {
public:
    BloomFilter(size_t n_bits, size_t k)
    : m_n_bits(n_bits), m_n_salts(k), m_bitarray(n_bits) {
        gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);
        salt = (uint16_t*) aligned_alloc(CACHELINE_SIZE, CACHELINEALIGN(k * sizeof(uint16_t)));
        for (size_t i = 0;  i < k; ++i) {
            salt[i] = (uint16_t) gsl_rng_uniform_int(rng, 1 << 16);
        }

        gsl_rng_free(rng);
    }

    BloomFilter(double max_fpr, size_t n, size_t k)
    : BloomFilter((size_t)(-(double) (k * n) / std::log(1.0 - std::pow(max_fpr, 1.0 / k))), k) {}

    ~BloomFilter() {
        if (salt) free(salt);
    }

    int insert(const K& key, size_t sz = sizeof(K)) {
        if (m_bitarray.size() == 0) return 0;

        for (size_t i = 0; i < m_n_salts; ++i) {
            m_bitarray.set(hash_bytes_with_salt((const char*)&key, sz, salt[i]) % m_n_bits);
        }

        return 1;
    }

    bool lookup(const K& key, size_t sz = sizeof(K)) {
        if (m_bitarray.size() == 0) return false;
        for (size_t i = 0; i < m_n_salts; ++i) {
            if (!m_bitarray.is_set(hash_bytes_with_salt((const char*)&key, sz, salt[i]) % m_n_bits))
                return false;
        }

        return true;
    }

    void clear() {
        m_bitarray.clear();
    }

    size_t get_memory_usage() {
        return this->m_bitarray.mem_size();
    }
private: 
    size_t m_n_salts;
    size_t m_n_bits;
    uint16_t* salt;

    BitArray m_bitarray;
};

}
