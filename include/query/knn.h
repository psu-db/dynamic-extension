/*
 * include/query/knn.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for k-NN queries, designed for use with the VPTree
 * shard.
 *
 * FIXME: no support for tombstone deletes just yet. This would require a
 * query resumption mechanism, most likely.
 */
#pragma once

#include "framework/QueryRequirements.h"
#include "psu-ds/PriorityQueue.h"

namespace de {
namespace knn {

using psudb::PriorityQueue;

template <ShardInterface S> class Query {
  typedef typename S::RECORD R;

public:
  struct Parameters {
    R point;
    size_t k;
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
  constexpr static bool EARLY_ABORT = false;
  constexpr static bool SKIP_DELETE_FILTER = true;

  static LocalQuery *local_preproc(S *shard, Parameters *parms) {
    auto query = new LocalQuery();
    query->global_parms = *parms;

    return query;
  }

  static LocalQueryBuffer *local_preproc_buffer(BufferView<R> *buffer,
                                                Parameters *parms) {
    auto query = new LocalQueryBuffer();
    query->global_parms = *parms;
    query->buffer = buffer;

    return query;
  }

  static void distribute_query(Parameters *parms,
                               std::vector<LocalQuery *> const &local_queries,
                               LocalQueryBuffer *buffer_query) {
    return;
  }

  static std::vector<LocalResultType> local_query(S *shard, LocalQuery *query) {
    std::vector<LocalResultType> results;

    Wrapped<R> wrec;
    wrec.rec = query->global_parms.point;
    wrec.header = 0;

    PriorityQueue<Wrapped<R>, DistCmpMax<Wrapped<R>>> pq(query->global_parms.k,
                                                         &wrec);

    shard->search(query->global_parms.point, query->global_parms.k, pq);

    while (pq.size() > 0) {
      results.emplace_back(*pq.peek().data);
      pq.pop();
    }

    return results;
  }

  static std::vector<LocalResultType>
  local_query_buffer(LocalQueryBuffer *query) {

    std::vector<LocalResultType> results;

    Wrapped<R> wrec;
    wrec.rec = query->global_parms.point;
    wrec.header = 0;

    PriorityQueue<Wrapped<R>, DistCmpMax<Wrapped<R>>> pq(query->global_parms.k,
                                                         &wrec);

    for (size_t i = 0; i < query->buffer->get_record_count(); i++) {
      // Skip over deleted records (under tagging)
      if (query->buffer->get(i)->is_deleted()) {
        continue;
      }

      if (pq.size() < query->global_parms.k) {
        pq.push(query->buffer->get(i));
      } else {
        double head_dist = pq.peek().data->rec.calc_distance(wrec.rec);
        double cur_dist = (query->buffer->get(i))->rec.calc_distance(wrec.rec);

        if (cur_dist < head_dist) {
          pq.pop();
          pq.push(query->buffer->get(i));
        }
      }
    }

    while (pq.size() > 0) {
      results.emplace_back(*(pq.peek().data));
      pq.pop();
    }

    return std::move(results);
  }

  static void
  combine(std::vector<std::vector<LocalResultType>> const &local_results,
          Parameters *parms, std::vector<ResultType> &output) {

    PriorityQueue<R, DistCmpMax<R>> pq(parms->k, &(parms->point));
    for (size_t i = 0; i < local_results.size(); i++) {
      for (size_t j = 0; j < local_results[i].size(); j++) {
        if (pq.size() < parms->k) {
          pq.push(&local_results[i][j].rec);
        } else {
          double head_dist = pq.peek().data->calc_distance(parms->point);
          double cur_dist = local_results[i][j].rec.calc_distance(parms->point);

          if (cur_dist < head_dist) {
            pq.pop();
            pq.push(&local_results[i][j].rec);
          }
        }
      }
    }

    while (pq.size() > 0) {
      output.emplace_back(*pq.peek().data);
      pq.pop();
    }
  }

  static bool repeat(Parameters *parms, std::vector<ResultType> &output,
                     std::vector<LocalQuery *> const &local_queries,
                     LocalQueryBuffer *buffer_query) {
    return false;
  }
};
} // namespace knn
} // namespace de
