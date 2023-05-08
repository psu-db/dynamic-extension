/*
 * include/util/hash.h
 *
 * Copyright (C) 2023 Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <cstdlib>
#include <cstdint>

namespace de {

// 40343 is a "magic constant" that works well,
// 38299 is another good value.
// Both are primes and have a good distribution of bits.
const uint64_t kHashMagicNum = 40343;

inline uint64_t rotr64(uint64_t x, size_t n) {
    return (((x) >> n) | ((x) << (64 - n)));
}

inline uint64_t hash(uint64_t input) {
    uint64_t local_rand = input;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return rotr64(local_rand_hash, 43);
}

inline uint64_t hash_bytes(const char* str, size_t len) {
    uint64_t hashState = len;

    for(size_t idx = 0; idx < len; ++idx) {
      hashState = kHashMagicNum * hashState + str[idx];
    }

    // The final scrambling helps with short keys that vary only on the high order bits.
    // Low order bits are not always well distributed so shift them to the high end, where they'll
    // form part of the 14-bit tag.
    return rotr64(kHashMagicNum * hashState, 6);
}

inline uint64_t hash_bytes_with_salt(const char* str, size_t len, uint16_t salt) {
    uint64_t hashState = len;

    for(size_t idx = 0; idx < len; ++idx) {
      hashState = kHashMagicNum * hashState + str[idx];
    }

    hashState = kHashMagicNum * hashState + salt;

    return rotr64(kHashMagicNum * hashState, 6);
}

}
