/*
 * include/util/Cursor.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A simple record cursor type with associated methods for help in
 * merging record sets when constructing shards. Iterates an array
 * of records in order, and provides facilities to make sorted merges
 * easier.
 *
 * TODO: Prior versions of this module included automatic support for
 * working with data stored in PagedFiles as well. That should be
 * reintroduced at some point.
 */
#pragma once

#include <cstdlib>
#include <vector>

namespace de {
template <typename R> struct Cursor {
  const R *ptr;
  const R *end;
  size_t cur_rec_idx;
  size_t rec_cnt;

  friend bool operator==(const Cursor &a, const Cursor &b) {
    return a.ptr == b.ptr && a.end == b.end;
  }
};

/*
 * Advance the cursor to the next record. If the cursor is backed by an
 * iterator, will attempt to advance the iterator once the cursor reaches its
 * end and reset the cursor to the beginning of the read page.
 *
 * If the advance succeeds, ptr will be updated to point to the new record
 * and true will be returned. If the advance reaches the end, then ptr will
 * be updated to be equal to end, and false will be returned. Iterators will
 * not be closed.
 */
template <typename R> inline static bool advance_cursor(Cursor<R> &cur) {
  cur.ptr++;
  cur.cur_rec_idx++;

  if (cur.cur_rec_idx >= cur.rec_cnt)
    return false;

  if (cur.ptr >= cur.end) {
    return false;
  }
  return true;
}

/*
 *   Process the list of cursors to return the cursor containing the next
 *   largest element. Does not advance any of the cursors. If current is
 *   specified, then skip the current head of that cursor during checking.
 *   This allows for "peaking" at the next largest element after the current
 *   largest is processed.
 */
template <typename R>
inline static Cursor<R> *get_next(std::vector<Cursor<R>> &cursors,
                                  Cursor<R> *current = nullptr) {
  const R *min_rec = nullptr;
  Cursor<R> *result = nullptr;
  // FIXME: for large cursor vectors, it may be worth it to use a
  //        PriorityQueue here instead of scanning.
  for (size_t i = 0; i < cursors.size(); i++) {
    if (cursors[i] == (Cursor<R>){0})
      continue;

    const R *rec =
        (&cursors[i] == current) ? cursors[i].ptr + 1 : cursors[i].ptr;
    if (rec >= cursors[i].end)
      continue;

    if (min_rec == nullptr) {
      result = &cursors[i];
      min_rec = rec;
      continue;
    }

    if (*rec < *min_rec) {
      result = &cursors[i];
      min_rec = rec;
    }
  }

  return result;
}

} // namespace de
