/*
 * include/query/rangecount.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for single dimensional range count queries. This query
 * requires that the shard support get_lower_bound(key) and
 * get_record_at(index).
 */
#pragma once

#include "framework/QueryRequirements.h"

namespace de {
namespace rc {

template <ShardInterface S, bool FORCE_SCAN = true> class Query {
  typedef typename S::RECORD R;

public:
  struct Parameters {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
  };

  struct LocalQuery {
    size_t start_idx;
    size_t stop_idx;
    Parameters global_parms;
  };

  struct LocalQueryBuffer {
    BufferView<R> *buffer;
    Parameters global_parms;
  };

  struct LocalResultType {
    size_t record_count;
    size_t tombstone_count;

    bool is_deleted() {return false;}
    bool is_tombstone() {return false;}
  };

  typedef size_t ResultType;
  constexpr static bool EARLY_ABORT = false;
  constexpr static bool SKIP_DELETE_FILTER = true;

  static LocalQuery *local_preproc(S *shard, Parameters *parms) {
    auto query = new LocalQuery();

    query->start_idx = shard->get_lower_bound(parms->lower_bound);
    query->stop_idx = shard->get_record_count();
    query->global_parms.lower_bound = parms->lower_bound;
    query->global_parms.upper_bound = parms->upper_bound;

    return query;
  }

  static LocalQueryBuffer *local_preproc_buffer(BufferView<R> *buffer,
                                                Parameters *parms) {
    auto query = new LocalQueryBuffer();
    query->buffer = buffer;
    query->global_parms.lower_bound = parms->lower_bound;
    query->global_parms.upper_bound = parms->upper_bound;

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
    size_t reccnt = 0;
    size_t tscnt = 0;

    /*
     * roll the pointer forward to the first record that is
     * greater than or equal to the lower bound.
     */
    while (ptr < shard->get_data() + query->stop_idx &&
           ptr->rec.key < query->global_parms.lower_bound) {
      ptr++;
    }

    while (ptr < shard->get_data() + query->stop_idx &&
           ptr->rec.key <= query->global_parms.upper_bound) {

      if (!ptr->is_deleted()) {
        reccnt++;

        if (ptr->is_tombstone()) {
          tscnt++;
        }
      }

      ptr++;
    }

    result.push_back({reccnt, tscnt});
    return result;
  }

  static std::vector<LocalResultType>
  local_query_buffer(LocalQueryBuffer *query) {

    std::vector<LocalResultType> result;
    size_t reccnt = 0;
    size_t tscnt = 0;
    for (size_t i = 0; i < query->buffer->get_record_count(); i++) {
      auto rec = query->buffer->get(i);
      if (rec->rec.key >= query->global_parms.lower_bound &&
          rec->rec.key <= query->global_parms.upper_bound) {
        if (!rec->is_deleted()) {
          reccnt++;
          if (rec->is_tombstone()) {
            tscnt++;
          }
        }
      }
    }

    result.push_back({reccnt, tscnt});

    return result;
  }

  static void
  combine(std::vector<std::vector<LocalResultType>> const &local_results,
          Parameters *parms, std::vector<ResultType> &output) {
    size_t reccnt = 0;
    size_t tscnt = 0;

    for (auto &local_result : local_results) {
      reccnt += local_result[0].record_count;
      tscnt += local_result[0].tombstone_count;
    }

    /* if more tombstones than results, clamp the output at 0 */
    if (tscnt > reccnt) {
      tscnt = reccnt;
    }

    output.push_back({reccnt - tscnt});
  }

  static bool repeat(Parameters *parms, std::vector<ResultType> &output,
                     std::vector<LocalQuery *> const &local_queries,
                     LocalQueryBuffer *buffer_query) {
    return false;
  }
};

} // namespace rc
} // namespace de
