/*
 * include/query/pointlookup.h
 *
 * Copyright (C) 2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for point lookup operations.
 *
 * TODO: Currently, this only supports point lookups for unique keys (which
 * is the case for the trie that we're building this to use). It would be
 * pretty straightforward to extend it to return *all* records that match
 * the search_key (including tombstone cancellation--it's invertible) to
 * support non-unique indexes, or at least those implementing
 * lower_bound().
 */
#pragma once

#include "framework/QueryRequirements.h"

namespace de {
namespace pl {

template <ShardInterface S> class Query {
  typedef typename S::RECORD R;

public:
  struct Parameters {
    decltype(R::key) search_key;
  };

  struct LocalQuery {
    Parameters global_parms;
  };

  struct LocalQueryBuffer {
    BufferView<R> *buffer;
    Parameters global_parms;
  };

  typedef Wrapped<R> LocalResultType;
  typedef R ResultType;
    
  constexpr static bool EARLY_ABORT = true;
  constexpr static bool SKIP_DELETE_FILTER = true;

  static LocalQuery *local_preproc(S *shard, Parameters *parms) {
    auto query = new LocalQuery();
    query->global_parms = *parms;
    return query;
  }

  static LocalQueryBuffer *local_preproc_buffer(BufferView<R> *buffer,
                                                Parameters *parms) {
    auto query = new LocalQueryBuffer();
    query->buffer = buffer;
    query->global_parms = *parms;

    return query;
  }
  
  static void distribute_query(Parameters *parms,
                               std::vector<LocalQuery *> const &local_queries,
                               LocalQueryBuffer *buffer_query) {
    return;
  }

  static std::vector<LocalResultType> local_query(S *shard, LocalQuery *query) {
    std::vector<LocalResultType> result;

    auto r = shard->point_lookup({query->global_parms.search_key, 0});

    if (r) {
      result.push_back(*r);
    }

    return result;
  }
  
  static std::vector<LocalResultType>
  local_query_buffer(LocalQueryBuffer *query) {
    std::vector<LocalResultType> result;

    for (size_t i = 0; i < query->buffer->get_record_count(); i++) {
      auto rec = query->buffer->get(i);

      if (rec->rec.key == query->global_parms.search_key) {
        result.push_back(*rec);
        return result;
      }
    }

    return result;
  }
  

  static void
  combine(std::vector<std::vector<LocalResultType>> const &local_results,
          Parameters *parms, std::vector<ResultType> &output) {
    for (auto r : local_results) {
      if (r.size() > 0) {
        if (r[0].is_deleted() || r[0].is_tombstone()) {
          return;
        }

        output.push_back(r[0].rec);
        return;
      }
    }
  }
    
  static bool repeat(Parameters *parms, std::vector<ResultType> &output,
                     std::vector<LocalQuery *> const &local_queries,
                     LocalQueryBuffer *buffer_query) {
    return false;
  }
};
} // namespace pl
} // namespace de
