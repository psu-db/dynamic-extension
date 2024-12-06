/*
 * include/framework/interface/Query.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/QueryRequirements.h"

namespace de {

/*
 * FIXME: It would probably be best to absorb the std::vector into
 *        this type too; this would allow user-defined collections for
 *        intermediate results, which could allow for more merging
 *        optimizations. However, this would require an alternative
 *        approach to doing delete checks, so we'll leave it for now.
 */
template <typename R>
concept LocalResultInterface = requires(R res) {
  { res.is_deleted() } -> std::convertible_to<bool>;
  { res.is_tombstone() } -> std::convertible_to<bool>;
};

/*
 *
 *
 */
template <typename QUERY, typename SHARD,
          typename RESULT = typename QUERY::ResultType,
          typename LOCAL_RESULT = typename QUERY::LocalResultType,
          typename PARAMETERS = typename QUERY::Parameters,
          typename LOCAL = typename QUERY::LocalQuery,
          typename LOCAL_BUFFER = typename QUERY::LocalQueryBuffer>
concept QueryInterface = LocalResultInterface<LOCAL_RESULT> &&
    requires(PARAMETERS *parameters, LOCAL *local, LOCAL_BUFFER *buffer_query,
             SHARD *shard, std::vector<LOCAL *> &local_queries,
             std::vector<std::vector<LOCAL_RESULT>> &local_results,
             std::vector<RESULT> &result,
             BufferView<typename SHARD::RECORD> *bv) {

  /*
   * Given a set of query parameters and a shard, return a local query
   * object for that shard.
   */
  { QUERY::local_preproc(shard, parameters) } -> std::convertible_to<LOCAL *>;

  /*
   * Given a set of query parameters and a buffer view, return a local
   * query object for the buffer.
   * NOTE: for interface reasons, the pointer to the buffer view MUST be
   *       stored inside of the local query object. The future buffer
   *       query routine will access the buffer by way of this pointer.
   */
  {
    QUERY::local_preproc_buffer(bv, parameters)
    } -> std::convertible_to<LOCAL_BUFFER *>;

  /*
   * Given a full set of local queries, and the buffer query, make any
   * necessary adjustments to the local queries in-place, to account for
   * global information. If no additional processing is required, this
   * function can be left empty.
   */
  {QUERY::distribute_query(parameters, local_queries, buffer_query)};

  /*
   * Answer the local query, defined by `local` against `shard` and return
   * a vector of LOCAL_RESULT objects defining the query result.
   */
  {
    QUERY::local_query(shard, local)
    } -> std::convertible_to<std::vector<LOCAL_RESULT>>;

  /*
   * Answer the local query defined by `local` against the buffer (which
   * should be accessed by a pointer inside of `local`) and return a vector
   * of LOCAL_RESULT objects defining the query result.
   */
  {
    QUERY::local_query_buffer(buffer_query)
    } -> std::convertible_to<std::vector<LOCAL_RESULT>>;

  /*
   * Process the local results from the buffer and all of the shards,
   * stored in `local_results`, and insert the associated ResultType
   * objects into the `result` vector, which represents the final result
   * of the query. Updates to this vector are done in-place.
   */
  {QUERY::combine(local_results, parameters, result)};

  /*
   * Process the post-combine `result` vector of ResultType objects,
   * in the context of the global and local query parameters, to determine
   * if the query should be repeated. If so, make any necessary adjustments
   * to the local query objects and return True. Otherwise, return False.
   *
   * If no repetition is needed for a given problem type, simply return
   * False immediately and the query will end.
   */
  {
    QUERY::repeat(parameters, result, local_queries, buffer_query)
    } -> std::same_as<bool>;

  /*
   * If this flag is True, then the query will immediately stop and return
   * a result as soon as the first non-deleted LocalRecordType is found.
   * Otherwise, every Shard and the buffer will be queried and the results
   * merged, like normal.
   *
   * This is largely an optimization flag for use with point-lookup, or
   * other single-record result queries
   */
  { QUERY::EARLY_ABORT } -> std::convertible_to<bool>;

  /*
   * If false, the built-in delete filtering that the framework can
   * apply to the local results, prior to calling combine, will be skipped.
   * This general filtering can be inefficient, particularly for tombstone
   * -based deletes, and so if a more efficient manual filtering can be
   * performed, it is worth setting this to True and doing that filtering
   * in the combine step.
   *
   * If deletes are not a consideration for your problem, it's also best
   * to turn this off, as it'll avoid the framework making an extra pass
   * over the local results prior to combining them.
   *
   * TODO: Temporarily disabling this, as we've dropped framework-level
   *       delete filtering for the time being.
   */
   /* { QUERY::SKIP_DELETE_FILTER } -> std::convertible_to<bool>; */
};
} // namespace de
