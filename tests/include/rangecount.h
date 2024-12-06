/*
 * tests/include/rangequery.h
 *
 * Standardized unit tests for range queries against supporting
 * shard types
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 * WARNING: This file must be included in the main unit test set
 *          after the definition of an appropriate Shard and R
 *          type. In particular, R needs to implement the key-value
 *          pair interface and Shard needs to support lower_bound. 
 *          For other types of record and shard, you'll need to
 *          use a different set of unit tests.
 */
#pragma once

#include "query/rangecount.h"
#include <algorithm>

/*
 * Uncomment these lines temporarily to remove errors in this file
 * temporarily for development purposes. They should be removed prior
 * to building, to ensure no duplicate definitions. These includes/defines
 * should be included in the source file that includes this one, above the
 * include statement.
 */
// #include "shard/ISAMTree.h"
// #include "query/rangequery.h"
// #include "testing.h"
// #include <check.h>
// using namespace de;

// typedef Rec R;
// typedef ISAMTree<R> Shard;
// typedef rc::Query<ISAMTree<R>> Query;

START_TEST(t_range_count)
{
    auto buffer = create_sequential_mbuffer<R>(100, 1000);
    auto shard = Shard(buffer->get_buffer_view());

    rc::Query<Shard>::Parameters parms = {300, 500};

    auto local_query = rc::Query<Shard>::local_preproc(&shard, &parms);
        
    auto result = rc::Query<Shard>::local_query(&shard, local_query);
    delete local_query;

    ck_assert_int_eq(result[0].record_count - result[0].tombstone_count, parms.upper_bound - parms.lower_bound + 1);

    delete buffer;
}
END_TEST


START_TEST(t_buffer_range_count)
{
    auto buffer = create_sequential_mbuffer<R>(100, 1000);

    rc::Query<Shard>::Parameters parms = {300, 500};

    {
        auto view = buffer->get_buffer_view();
        auto query = rc::Query<Shard>::local_preproc_buffer(&view, &parms);
        auto result = rc::Query<Shard>::local_query_buffer(query); 
        delete query;

        ck_assert_int_eq(result[0].record_count - result[0].tombstone_count, parms.upper_bound - parms.lower_bound + 1);
    }

    delete buffer;
}
END_TEST


START_TEST(t_range_count_merge)
{    
    auto buffer1 = create_sequential_mbuffer<R>(100, 200);
    auto buffer2 = create_sequential_mbuffer<R>(400, 1000);

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    rc::Query<Shard>::Parameters parms = {150, 500};

    size_t result_size = parms.upper_bound - parms.lower_bound + 1 - 200;

    auto query1 = rc::Query<Shard>::local_preproc(&shard1, &parms);
    auto query2 = rc::Query<Shard>::local_preproc(&shard2, &parms);

    std::vector<std::vector<rc::Query<Shard>::LocalResultType>> results(2);
    results[0] = rc::Query<Shard>::local_query(&shard1, query1);     
    results[1] = rc::Query<Shard>::local_query(&shard2, query2); 
    delete query1;
    delete query2;

    size_t reccnt = results[0][0].record_count + results[1][0].record_count;
    size_t tscnt = results[0][0].tombstone_count + results[1][0].tombstone_count;

    ck_assert_int_eq(reccnt - tscnt, result_size);

    std::vector<rc::Query<Shard>::ResultType> result;
    rc::Query<Shard>::combine(results, nullptr, result);

    ck_assert_int_eq(result[0], result_size);

    delete buffer1;
    delete buffer2;
}
END_TEST

static void inject_rangecount_tests(Suite *suite) {
    TCase *range_count = tcase_create("Range Query Testing"); 
    tcase_add_test(range_count, t_range_count); 
    tcase_add_test(range_count, t_buffer_range_count); 
    tcase_add_test(range_count, t_range_count_merge); 
    suite_add_tcase(suite, range_count);
}
