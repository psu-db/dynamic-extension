/*
 * include/query/irs.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A query class for independent range sampling. This query requires
 * that the shard support get_lower_bound(key), get_upper_bound(key),
 * and get_record_at(index).
 */
#pragma once

#include "framework/QueryRequirements.h"
#include "psu-ds/Alias.h"

namespace de {
namespace irs {

template <ShardInterface S, bool REJECTION = true> class Query {
  typedef typename S::RECORD R;

public:
  struct Parameters {
    decltype(R::key) lower_bound;
    decltype(R::key) upper_bound;
    size_t sample_size;
    gsl_rng *rng;
  };

  struct LocalQuery {
    size_t lower_idx;
    size_t upper_idx;
    size_t total_weight;
    size_t sample_size;
    Parameters global_parms;
  };

  struct LocalQueryBuffer {
    BufferView<R> *buffer;

    size_t cutoff;
    std::vector<Wrapped<R>> records;
    std::unique_ptr<psudb::Alias> alias;
    size_t sample_size;

    Parameters global_parms;
  };

  typedef Wrapped<R> LocalResultType;
  typedef R ResultType;

  constexpr static bool EARLY_ABORT = false;
  constexpr static bool SKIP_DELETE_FILTER = false;

  static LocalQuery *local_preproc(S *shard, Parameters *parms) {
    auto query = new LocalQuery();

    query->global_parms = *parms;

    query->lower_idx = shard->get_lower_bound(query->global_parms.lower_bound);
    query->upper_idx = shard->get_upper_bound(query->global_parms.upper_bound);

    if (query->lower_idx == shard->get_record_count()) {
      query->total_weight = 0;
    } else {
      query->total_weight = query->upper_idx - query->lower_idx; 
    }

    query->sample_size = 0;
    return query;
  }

  static LocalQueryBuffer *local_preproc_buffer(BufferView<R> *buffer,
                                                Parameters *parms) {
    auto query = new LocalQueryBuffer();
    query->buffer = buffer;

    query->cutoff = query->buffer->get_record_count();
    query->sample_size = 0;
    query->alias = nullptr;
    query->global_parms = *parms;

    if constexpr (REJECTION) {
      return query;
    }

    for (size_t i = 0; i < query->cutoff; i++) {
      if ((query->buffer->get(i)->rec.key >= query->global_parms.lower_bound) &&
          (buffer->get(i)->rec.key <= query->global_parms.upper_bound)) {
        query->records.emplace_back(*(query->buffer->get(i)));
      }
    }

    return query;
  }

  static void distribute_query(Parameters *parms,
                               std::vector<LocalQuery *> const &local_queries,
                               LocalQueryBuffer *buffer_query) {

    std::vector<size_t> shard_sample_sizes(local_queries.size() + 1, 0);
    size_t buffer_sz = 0;

    /* for simplicity of static structure testing */
    if (!buffer_query) {
      assert(local_queries.size() == 1);
      local_queries[0]->sample_size =
          local_queries[0]->global_parms.sample_size;
      return;
    }

    /* we only need to build the shard alias on the first call */
    if (buffer_query->alias == nullptr) {
      std::vector<size_t> weights;
      if constexpr (REJECTION) {
        weights.push_back(buffer_query->cutoff);
      } else {
        weights.push_back(buffer_query->records.size());
      }

      size_t total_weight = weights[0];
      for (auto &q : local_queries) {
        total_weight += q->total_weight;
        weights.push_back(q->total_weight);
      }

      /*
       * if no valid records fall within the query range,
       * set all of the sample sizes to 0 and bail out.
       */
      if (total_weight == 0) {
        for (auto q : local_queries) {
          q->sample_size = 0;
        }

        return;
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
        buffer_sz++;
      } else {
        shard_sample_sizes[idx - 1]++;
      }
    }

    if (buffer_query) {
      buffer_query->sample_size = buffer_sz;
    }

    for (size_t i = 0; i < local_queries.size(); i++) {
      local_queries[i]->sample_size = shard_sample_sizes[i];
    }
  }

  static std::vector<LocalResultType> local_query(S *shard, LocalQuery *query) {
    auto sample_sz = query->sample_size;

    std::vector<LocalResultType> result_set;

    if (sample_sz == 0 || query->lower_idx == shard->get_record_count()) {
      return result_set;
    }

    size_t attempts = 0;
    size_t range_length = query->upper_idx - query->lower_idx;
    do {
      attempts++;
      size_t idx =
          (range_length > 0)
              ? gsl_rng_uniform_int(query->global_parms.rng, range_length)
              : 0;
      result_set.emplace_back(*shard->get_record_at(query->lower_idx + idx));
    } while (attempts < sample_sz);

    return result_set;
  }

  static std::vector<LocalResultType>
  local_query_buffer(LocalQueryBuffer *query) {
    std::vector<LocalResultType> result;
    result.reserve(query->sample_size);

    if constexpr (REJECTION) {
      for (size_t i = 0; i < query->sample_size; i++) {
        auto idx = gsl_rng_uniform_int(query->global_parms.rng, query->cutoff);
        auto rec = query->buffer->get(idx);

        if (rec->rec.key >= query->global_parms.lower_bound &&
            rec->rec.key <= query->global_parms.upper_bound) {
          result.emplace_back(*rec);
        }
      }

      return result;
    }

    for (size_t i = 0; i < query->sample_size; i++) {
      auto idx =
          gsl_rng_uniform_int(query->global_parms.rng, query->records.size());
      result.emplace_back(query->records[idx]);
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
} // namespace irs
} // namespace de
