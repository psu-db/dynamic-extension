/*
 * include/util/internal_record.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once
#pragma once

#include "util/record.h"
#include "util/types.h"

/*
 * Utility functions for use in handling internal nodes within an ISAM
 * tree.
 */

namespace de {

struct ISAMTreeInternalNodeHeader {
    PageNum next_sibling; // INVALID_PNUM this is the last node on a level
    PageNum prev_sibling; // INVALID_PNUM if this is the first node on a level
    size_t leaf_rec_cnt; // number of records in leaf nodes under this node
    size_t internal_rec_cnt; // number of internal records in this node
};

/*
 * The total (aligned) size of an ISAMTreeInternalNodeHeader object. 
 */
static constexpr PageOffset ISAMTreeInternalNodeHeaderSize = MAXALIGN(sizeof(ISAMTreeInternalNodeHeader));

/*
 * The number of bytes occupied by an ISAM tree internal record, including alignment bytes.
 */
static constexpr size_t internal_record_size = sizeof(key_t) + MAXALIGN(sizeof(PageNum));

/*
 * The maximum number of internal records that can be stored in an internal
 * node, accounting for alignment and the node header.
 */
static constexpr size_t internal_records_per_page = (PAGE_SIZE - ISAMTreeInternalNodeHeaderSize) / internal_record_size;

/*
 * Format an internal record, starting in the first byte of buffer, with the
 * specified key and target page number. If either buffer or key refer to
 * memory regions of insufficient size, the results are undefined.
 */
static inline void build_internal_record(char *buffer, const char *key, PageNum target_page) {
    memcpy(buffer, key, sizeof(key_t));
    memcpy(buffer + sizeof(key_t), &target_page, sizeof(PageNum));
}

/*
 * Return the idx'th internal record within an internal node.
 * internal_page_buffer must refer to the first byte of a page containing a
 * full internal node, including the header. The returned value is undefined if
 * idx is greater than or equal to the number of internal records within the
 * node.
 */
static inline char *get_internal_record(char *internal_page_buffer, size_t idx) {
    return internal_page_buffer + ISAMTreeInternalNodeHeaderSize + internal_record_size * idx;
}

/*
 * Return a pointer to the key contained within an internal record, pointed
 * to by buffer. Buffer must point to the beginning of a valid internal record,
 * or the result of this function is undefined.
 */
static inline const char *get_internal_key(const char *buffer) {
    return buffer;
}

/*
 * Return the Page Number (value) of an internal record, pointed to by buffer.
 * Buffer must point to the beginning of a valid internal record, or the result
 * of this function is undefined.
 */
static inline PageNum get_internal_value(const char *buffer) {
    return *((PageNum *) (buffer + sizeof(key_t)));
}

/*
 * Return a pointer to the header of an ISAM Tree internal node, referred to by
 * buffer. If buffer contains multiple nodes, a specific node can be requested 
 * using the idx parameter, otherwise the header of the first node will be returned.
 * Buffer must point to the beginning of a valid ISAM Tree internal node, and idx
 * must be less than the number of internal nodes within the buffer, or the 
 * returned value is undefined.
 */
static inline ISAMTreeInternalNodeHeader *get_header(char *buffer, size_t idx=0) {
    return (ISAMTreeInternalNodeHeader *) get_page(buffer, idx);
}

}
