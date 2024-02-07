/*
 * tests/include/rangecount.h
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

/*
 * Uncomment these lines temporarily to remove errors in this file
 * temporarily for development purposes. They should be removed prior
 * to building, to ensure no duplicate definitions. These includes/defines
 * should be included in the source file that includes this one, above the
 * include statement.
 */
//#include "shard/ISAMTree.h"
//#include "query/rangecount.h"
//#include "testing.h"
//#include <check.h>
//using namespace de;
//typedef ISAMTree<R> Shard;


#include "query/rangecount.h"

START_TEST(t_range_count)
{
    
    auto buffer = create_sequential_mbuffer<R>(100, 1000);
    auto shard = Shard(buffer->get_buffer_view());

    rc::Parms<R> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rc::Query<R, Shard>::get_query_state(&shard, &parms);
    auto result = rc::Query<R, Shard>::query(&shard, state, &parms);
    rc::Query<R, Shard>::delete_query_state(state);

    ck_assert_int_eq(result.size(), 1);
    ck_assert_int_eq(result[0].rec.key, parms.upper_bound - parms.lower_bound + 1);

    delete buffer;
}
END_TEST


START_TEST(t_buffer_range_count)
{
    auto buffer = create_sequential_mbuffer<R>(100, 1000);

    rc::Parms<R> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    {
        auto view = buffer->get_buffer_view();
        auto state = rc::Query<R, Shard>::get_buffer_query_state(&view, &parms);
        auto result = rc::Query<R, Shard>::buffer_query(state, &parms);
        rc::Query<R, Shard>::delete_buffer_query_state(state);

        ck_assert_int_eq(result.size(), 1);
        ck_assert_int_eq(result[0].rec.key, parms.upper_bound - parms.lower_bound + 1);
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

    rc::Parms<R> parms;
    parms.lower_bound = 150;
    parms.upper_bound = 500;

    size_t result_size = parms.upper_bound - parms.lower_bound + 1 - 200;

    auto state1 = rc::Query<R, Shard>::get_query_state(&shard1, &parms);
    auto state2 = rc::Query<R, Shard>::get_query_state(&shard2, &parms);

    std::vector<std::vector<de::Wrapped<R>>> results(2);
    results[0] = rc::Query<R, Shard>::query(&shard1, state1, &parms);
    results[1] = rc::Query<R, Shard>::query(&shard2, state2, &parms);

    rc::Query<R, Shard>::delete_query_state(state1);
    rc::Query<R, Shard>::delete_query_state(state2);

    ck_assert_int_eq(results[0].size(), 1);
    ck_assert_int_eq(results[1].size(), 1);

    auto result = rc::Query<R, Shard>::merge(results, nullptr);

    ck_assert_int_eq(result[0].key, result_size);

    delete buffer1;
    delete buffer2;
}
END_TEST


START_TEST(t_lower_bound)
{
    auto buffer1 = create_sequential_mbuffer<R>(100, 200);
    auto buffer2 = create_sequential_mbuffer<R>(400, 1000);

    auto shard1 = new Shard(buffer1->get_buffer_view());
    auto shard2 = new Shard(buffer2->get_buffer_view());

    std::vector<Shard*> shards = {shard1, shard2};

    auto merged = Shard(shards);

    for (size_t i=100; i<1000; i++) {
        R r;
        r.key = i;
        r.value = i;

        auto idx = merged.get_lower_bound(i);

        assert(idx < merged.get_record_count());

        auto res = merged.get_record_at(idx);

        if (i >=200 && i <400) {
            ck_assert_int_lt(res->rec.key, i);
        } else {
            ck_assert_int_eq(res->rec.key, i);
        }
    }

    delete buffer1;
    delete buffer2;
    delete shard1;
    delete shard2;
}
END_TEST

static void inject_rangecount_tests(Suite *suite) {
    TCase *range_count = tcase_create("Range Query Testing"); 
    tcase_add_test(range_count, t_range_count); 
    tcase_add_test(range_count, t_buffer_range_count); 
    tcase_add_test(range_count, t_range_count_merge); 
    suite_add_tcase(suite, range_count);
}
