/*
 * include/query/wss.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for weighted set sampling. This
 * class is tightly coupled with include/shard/Alias.h,
 * and so is probably of limited general utility.
 */
#pragma once

#include "framework/QueryRequirements.h"
#include "psu-ds/Alias.h"

namespace de {
namespace wss {

template <ShardInterface S> class Query {
  typedef typename S::RECORD R;

public:
  struct Parameters {
    size_t sample_size;
    gsl_rng *rng;
  };

  struct LocalQuery {
    size_t sample_size;
    decltype(R::weight) total_weight;

    Parameters global_parms;
  };

  struct LocalQueryBuffer {
    BufferView<R> *buffer;

    size_t sample_size;
    decltype(R::weight) total_weight;
    decltype(R::weight) max_weight;
    size_t cutoff;

    std::unique_ptr<psudb::Alias> alias;

    Parameters global_parms;
  };

  constexpr static bool EARLY_ABORT = false;
  constexpr static bool SKIP_DELETE_FILTER = false;

  typedef Wrapped<R> LocalResultType;
  typedef R ResultType;

  static LocalQuery *local_preproc(S *shard, Parameters *parms) {
    auto query = new LocalQuery();

    query->global_parms = *parms;
    query->total_weight = shard->get_total_weight();
    query->sample_size = 0;

    return query;
  }

  static LocalQueryBuffer *local_preproc_buffer(BufferView<R> *buffer,
                                                Parameters *parms) {
    auto query = new LocalQueryBuffer();

    query->cutoff = buffer->get_record_count() - 1;

    query->max_weight = 0;
    query->total_weight = 0;

    for (size_t i = 0; i < buffer->get_record_count(); i++) {
      auto weight = buffer->get(i)->rec.weight;
      query->total_weight += weight;

      if (weight > query->max_weight) {
        query->max_weight = weight;
      }
    }

    query->buffer = buffer;
    query->global_parms = *parms;

    query->alias = nullptr;

    return query;
  }

  static void distribute_query(Parameters *parms,
                               std::vector<LocalQuery *> const &local_queries,
                               LocalQueryBuffer *buffer_query) {

    if (!buffer_query) {
      assert(local_queries.size() == 1);
      local_queries[0]->sample_size =
          local_queries[0]->global_parms.sample_size;
      return;
    }

    if (!buffer_query->alias) {
      std::vector<decltype(R::weight)> weights;

      decltype(R::weight) total_weight = buffer_query->total_weight;
      weights.push_back(total_weight);

      for (auto &q : local_queries) {
        total_weight += q->total_weight;
        weights.push_back(q->total_weight);
        q->sample_size = 0;
      }

      std::vector<double> normalized_weights;
      for (auto w : weights) {
        normalized_weights.push_back((double)w / (double)total_weight);
      }

      buffer_query->alias = std::make_unique<psudb::Alias>(normalized_weights);
    }

    for (size_t i = 0; i < parms->sample_size; i++) {
      auto idx = buffer_query->alias->get(parms->rng);

      if (idx == 0) {
        buffer_query->sample_size++;
      } else {
        local_queries[idx - 1]->sample_size++;
      }
    }
  }

  static std::vector<LocalResultType> local_query(S *shard, LocalQuery *query) {
    std::vector<LocalResultType> result;

    if (query->sample_size == 0) {
      return result;
    }

    for (size_t i = 0; i < query->sample_size; i++) {
      size_t idx = shard->get_weighted_sample(query->global_parms.rng);
      if (!shard->get_record_at(idx)->is_deleted()) {
        result.emplace_back(*shard->get_record_at(idx));
      }
    }

    return result;
  }

  static std::vector<LocalResultType>
  local_query_buffer(LocalQueryBuffer *query) {
    std::vector<LocalResultType> result;

    for (size_t i = 0; i < query->sample_size; i++) {
      auto idx = gsl_rng_uniform_int(query->global_parms.rng, query->cutoff);
      auto rec = query->buffer->get(idx);

      auto test = gsl_rng_uniform(query->global_parms.rng) * query->max_weight;
      if (test <= rec->rec.weight && !rec->is_deleted()) {
        result.emplace_back(*rec);
      }
    }

    return result;
  }

  static void
  combine(std::vector<std::vector<LocalResultType>> const &local_results,
          Parameters *parms, std::vector<ResultType> &output) {
    for (size_t i = 0; i < local_results.size(); i++) {
      for (size_t j = 0; j < local_results[i].size(); j++) {
        output.emplace_back(local_results[i][j].rec);
      }
    }
  }

  static bool repeat(Parameters *parms, std::vector<ResultType> &output,
                     std::vector<LocalQuery *> const &local_queries,
                     LocalQueryBuffer *buffer_query) {
    if (output.size() < parms->sample_size) {
      parms->sample_size -= output.size();
      distribute_query(parms, local_queries, buffer_query);
      return true;
    }

    return false;
  }
};
} // namespace wss
} // namespace de
