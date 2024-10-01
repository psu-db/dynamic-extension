/*
 * include/query/rangequery.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for single dimensional range queries. This query requires
 * that the shard support get_lower_bound(key) and get_record_at(index).
 */
#pragma once

#include "framework/QueryRequirements.h"
#include "framework/interface/Record.h"
#include "psu-ds/PriorityQueue.h"
#include "util/Cursor.h"

namespace de {
namespace rq {

template <ShardInterface S> 
class Query {
  typedef typename S::RECORD R;
public:
  struct LocalQuery {
    size_t start_idx;
    size_t stop_idx;
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
  };

  struct LocalQueryBuffer {
    BufferView<R> *buffer;
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
  };

  typedef Wrapped<R> LocalResultType;
  typedef R ResultType;

  struct Parameters {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
  };

  constexpr static bool EARLY_ABORT = false;
  constexpr static bool SKIP_DELETE_FILTER = true;

  static LocalQuery *local_preproc(S *shard, Parameters *parms) {
    auto query = new LocalQuery();

    query->start_idx = shard->get_lower_bound(parms->lower_bound);
    query->stop_idx = shard->get_record_count();
    query->lower_bound = parms->lower_bound;
    query->upper_bound = parms->upper_bound;

    return query;
  }

  static LocalQueryBuffer *local_preproc_buffer(BufferView<R> *buffer, Parameters *parms) {
    auto query = new LocalQueryBuffer();
    query->buffer = buffer;
    query->lower_bound = parms->lower_bound;
    query->upper_bound = parms->upper_bound;

    return query;
  }

  static void distribute_query(Parameters *parms,
                               std::vector<LocalQuery *> const &local_queries,
                               LocalQueryBuffer *buffer_query) {
    return;
  }

  static std::vector<LocalResultType> local_query(S *shard, LocalQuery *query) {
    std::vector<LocalResultType> result;

    /*
     * if the returned index is one past the end of the
     * records for the PGM, then there are not records
     * in the index falling into the specified range.
     */
    if (query->start_idx == shard->get_record_count()) {
      return result;
    }

    auto ptr = shard->get_record_at(query->start_idx);

    /*
     * roll the pointer forward to the first record that is
     * greater than or equal to the lower bound.
     */
    while (ptr < shard->get_data() + query->stop_idx &&
           ptr->rec.key < query->lower_bound) {
      ptr++;
    }

    while (ptr < shard->get_data() + query->stop_idx &&
           ptr->rec.key <= query->upper_bound) {
      result.emplace_back(*ptr);
      ptr++;
    }

    return result;
  }

  static std::vector<LocalResultType>
  local_query_buffer(LocalQueryBuffer *query) {

    std::vector<LocalResultType> result;
    for (size_t i = 0; i < query->buffer->get_record_count(); i++) {
      auto rec = query->buffer->get(i);
      if (rec->rec.key >= query->lower_bound &&
          rec->rec.key <= query->upper_bound) {
        result.emplace_back(*rec);
      }
    }

    return result;
  }

  static void combine(std::vector<std::vector<LocalResultType>> const &local_results,
                      Parameters *parms, std::vector<ResultType> &output) {
    std::vector<Cursor<LocalResultType>> cursors;
    cursors.reserve(local_results.size());

    psudb::PriorityQueue<LocalResultType> pq(local_results.size());
    size_t total = 0;
    size_t tmp_n = local_results.size();

    for (size_t i = 0; i < tmp_n; ++i)
      if (local_results[i].size() > 0) {
        auto base = local_results[i].data();
        cursors.emplace_back(Cursor<LocalResultType>{
            base, base + local_results[i].size(), 0, local_results[i].size()});
        assert(i == cursors.size() - 1);
        total += local_results[i].size();
        pq.push(cursors[i].ptr, tmp_n - i - 1);
      } else {
        cursors.emplace_back(Cursor<LocalResultType>{nullptr, nullptr, 0, 0});
      }

    if (total == 0) {
      return;
    }

    output.reserve(total);

    while (pq.size()) {
      auto now = pq.peek();
      auto next = pq.size() > 1
                      ? pq.peek(1)
                      : psudb::queue_record<LocalResultType>{nullptr, 0};
      if (!now.data->is_tombstone() && next.data != nullptr &&
          now.data->rec == next.data->rec && next.data->is_tombstone()) {

        pq.pop();
        pq.pop();
        auto &cursor1 = cursors[tmp_n - now.version - 1];
        auto &cursor2 = cursors[tmp_n - next.version - 1];
        if (advance_cursor<LocalResultType>(cursor1))
          pq.push(cursor1.ptr, now.version);
        if (advance_cursor<LocalResultType>(cursor2))
          pq.push(cursor2.ptr, next.version);
      } else {
        auto &cursor = cursors[tmp_n - now.version - 1];
        if (!now.data->is_tombstone())
          output.push_back(cursor.ptr->rec);

        pq.pop();

        if (advance_cursor<LocalResultType>(cursor))
          pq.push(cursor.ptr, now.version);
      }
    }

    return;
  }

  static bool repeat(Parameters *parms, std::vector<ResultType> &output,
                     std::vector<LocalQuery *> const &local_queries, LocalQueryBuffer *buffer_query) {
    return false;
  }
};

} // namespace rq
} // namespace de
