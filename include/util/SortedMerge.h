/*
 * include/util/SortedMerge.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 * A sorted array merge routine for use in Shard construction, as many
 * shards will use a sorted array to represent their data. Also encapsulates 
 * the necessary tombstone-cancellation logic.
 *
 * FIXME: include generic per-record processing functionality for Shards that 
 * need it, to avoid needing to reprocess the array in the shard after
 * creation.
 */
#pragma once

#include "util/Cursor.h"
#include "framework/interface/Shard.h"
#include "psu-ds/PriorityQueue.h"

namespace de {

using psudb::PriorityQueue;
using psudb::BloomFilter;
using psudb::queue_record;
using psudb::byte;
using psudb::CACHELINE_SIZE;

/*
 * A simple struct to return record_count and tombstone_count information 
 * back to the caller. Could've been an std::pair, but I like the more 
 * explicit names.
 */
struct merge_info {
    size_t record_count;
    size_t tombstone_count;
};

/*
 * Build a vector of cursors corresponding to the records contained within
 * a vector of shards. The cursor at index i in the output will correspond
 * to the shard at index i in the input. 
 *
 * The values of reccnt and tscnt will be updated with the sum of the
 * records contained within the shards. Note that these counts include deleted
 * records that may be removed during shard construction, and so constitute
 * upper bounds only.
 */
template <RecordInterface R, ShardInterface<R> S>
static std::vector<Cursor<Wrapped<R>>> build_cursor_vec(std::vector<S*> &shards, size_t *reccnt, size_t *tscnt) {
    std::vector<Cursor<Wrapped<R>>> cursors;
    cursors.reserve(shards.size());

    *reccnt = 0;
    *tscnt = 0;
    
    for (size_t i = 0; i < shards.size(); ++i) {
        if (shards[i]) {
            auto base = shards[i]->get_data();
            cursors.emplace_back(Cursor{base, base + shards[i]->get_record_count(), 0, shards[i]->get_record_count()});
            *reccnt += shards[i]->get_record_count();
            *tscnt += shards[i]->get_tombstone_count();
        } else {
            cursors.emplace_back(Cursor<Wrapped<R>>{nullptr, nullptr, 0, 0});
        }
    }

    return cursors;
}

/*
 * Build a sorted array of records based on the contents of a BufferView.
 * This routine does not alter the buffer view, but rather copies the
 * records out and then sorts them. The provided buffer must be large
 * enough to store the records from the BufferView, or the behavior of the
 * function is undefined.
 *
 * It allocates a temporary buffer for the sorting, and execution of the
 * program will be aborted if the allocation fails.
 */
template <RecordInterface R>
static merge_info sorted_array_from_bufferview(BufferView<R> bv, 
                                               Wrapped<R> *buffer,
                                               psudb::BloomFilter<R> *bf=nullptr) {
    /*
     * Copy the contents of the buffer view into a temporary buffer, and
     * sort them. We still need to iterate over these temporary records to 
     * apply tombstone/deleted record filtering, as well as any possible
     * per-record processing that is required by the shard being built.
     */
    auto temp_buffer = (Wrapped<R> *) psudb::sf_aligned_calloc(CACHELINE_SIZE, 
                                                               bv.get_record_count(), 
                                                               sizeof(Wrapped<R>));
    bv.copy_to_buffer((byte *) temp_buffer);

    auto base = temp_buffer;
    auto stop = base + bv.get_record_count();
    std::sort(base, stop, std::less<Wrapped<R>>());

    merge_info info = {0, 0};

    /* 
     * Iterate over the temporary buffer to process the records, copying
     * them into buffer as needed
     */
    while (base < stop) {
        if (!base->is_tombstone() && (base + 1 < stop)
            && base->rec == (base + 1)->rec  && (base + 1)->is_tombstone()) {
            base += 2;
            continue;
        } else if (base->is_deleted()) {
            base += 1;
            continue;
        }

        // FIXME: this shouldn't be necessary, but the tagged record
        // bypass doesn't seem to be working on this code-path, so this
        // ensures that tagged records from the buffer are able to be
        // dropped, eventually. It should only need to be &= 1
        base->header &= 3;
        buffer[info.record_count++] = *base;

        if (base->is_tombstone()) {
            info.tombstone_count++;
            if (bf){
                bf->insert(base->rec);
            }
        }

        base++;
    }

    free(temp_buffer);
    return info;
}

/*
 * Perform a sorted merge of the records within cursors into the provided
 * buffer. Includes tombstone and tagged delete cancellation logic, and
 * will insert tombstones into a bloom filter, if one is provided. 
 *
 * The behavior of this function is undefined if the provided buffer does
 * not have space to contain all of the records within the input cursors.
 */
template <RecordInterface R>
static merge_info sorted_array_merge(std::vector<Cursor<Wrapped<R>>> &cursors, 
                              Wrapped<R> *buffer, 
                              psudb::BloomFilter<R> *bf=nullptr) {
    
    // FIXME: For smaller cursor arrays, it may be more efficient to skip
    //        the priority queue and just do a scan.
    PriorityQueue<Wrapped<R>> pq(cursors.size());
    for (size_t i=0; i<cursors.size(); i++) {
        pq.push(cursors[i].ptr, i);
    }

    merge_info info = {0, 0};
    while (pq.size()) {
        auto now = pq.peek();
        auto next = pq.size() > 1 ? pq.peek(1) : queue_record<Wrapped<R>>{nullptr, 0};
        /* 
         * if the current record is not a tombstone, and the next record is
         * a tombstone that matches the current one, then the current one
         * has been deleted, and both it and its tombstone can be skipped
         * over.
         */
        if (!now.data->is_tombstone() && next.data != nullptr &&
            now.data->rec == next.data->rec && next.data->is_tombstone()) {
            
            pq.pop(); pq.pop();
            auto& cursor1 = cursors[now.version];
            auto& cursor2 = cursors[next.version];
            if (advance_cursor(cursor1)) pq.push(cursor1.ptr, now.version);
            if (advance_cursor(cursor2)) pq.push(cursor2.ptr, next.version);
        } else {
            auto& cursor = cursors[now.version];
            /* skip over records that have been deleted via tagging */
            if (!cursor.ptr->is_deleted()) {
                buffer[info.record_count++] = *cursor.ptr;

                /*  
                 * if the record is a tombstone, increment the ts count and 
                 * insert it into the bloom filter if one has been
                 * provided.
                 */
                if (cursor.ptr->is_tombstone()) {
                    info.tombstone_count++;
                    if (bf) {
                        bf->insert(cursor.ptr->rec);
                    }
                }
            }
            pq.pop();
            
            if (advance_cursor(cursor)) pq.push(cursor.ptr, now.version);
        }
    }

    return info;
}



}
