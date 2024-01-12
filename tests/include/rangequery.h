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
 *          after the definition of an appropriate Shard and Rec
 *          type. In particular, Rec needs to implement the key-value
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
//#include "query/rangequery.h"
//#include "testing.h"
//#include <check.h>
//using namespace de;
//typedef ISAMTree<Rec> Shard;


START_TEST(t_range_query)
{
    auto buffer = create_sequential_mbuffer<Rec>(100, 1000);
    auto shard = Shard(buffer->get_buffer_view());

    rq::Parms<Rec> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rq::Query<Shard, Rec>::get_query_state(&shard, &parms);
    auto result = rq::Query<Shard, Rec>::query(&shard, state, &parms);
    rq::Query<Shard, Rec>::delete_query_state(state);

    ck_assert_int_eq(result.size(), parms.upper_bound - parms.lower_bound + 1);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_buffer_range_query)
{
    auto buffer = create_sequential_mbuffer<Rec>(100, 1000);

    rq::Parms<Rec> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rq::Query<Shard, Rec>::get_buffer_query_state(buffer->get_buffer_view(), &parms);
    auto result = rq::Query<Shard, Rec>::buffer_query(state, &parms);
    rq::Query<Shard, Rec>::delete_buffer_query_state(state);

    ck_assert_int_eq(result.size(), parms.upper_bound - parms.lower_bound + 1);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_range_query_merge)
{    
    auto buffer1 = create_sequential_mbuffer<Rec>(100, 200);
    auto buffer2 = create_sequential_mbuffer<Rec>(400, 1000);

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    rq::Parms<Rec> parms;
    parms.lower_bound = 150;
    parms.upper_bound = 500;

    size_t result_size = parms.upper_bound - parms.lower_bound + 1 - 200;

    auto state1 = rq::Query<Shard, Rec>::get_query_state(&shard1, &parms);
    auto state2 = rq::Query<Shard, Rec>::get_query_state(&shard2, &parms);

    std::vector<std::vector<de::Wrapped<Rec>>> results(2);
    results[0] = rq::Query<Shard, Rec>::query(&shard1, state1, &parms);
    results[1] = rq::Query<Shard, Rec>::query(&shard2, state2, &parms);

    rq::Query<Shard, Rec>::delete_query_state(state1);
    rq::Query<Shard, Rec>::delete_query_state(state2);

    ck_assert_int_eq(results[0].size() + results[1].size(), result_size);

    std::vector<std::vector<Wrapped<Rec>>> proc_results;

    for (size_t j=0; j<results.size(); j++) {
        proc_results.emplace_back(std::vector<Wrapped<Rec>>());
        for (size_t i=0; i<results[j].size(); i++) {
            proc_results[j].emplace_back(results[j][i]);
        }
    }

    auto result = rq::Query<Shard, Rec>::merge(proc_results, nullptr);
    std::sort(result.begin(), result.end());

    ck_assert_int_eq(result.size(), result_size);
    auto key = parms.lower_bound;
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_eq(key++, result[i].key);
        if (key == 200) {
            key = 400;
        }
    }

    delete buffer1;
    delete buffer2;
}
END_TEST


START_TEST(t_lower_bound)
{
    auto buffer1 = create_sequential_mbuffer<Rec>(100, 200);
    auto buffer2 = create_sequential_mbuffer<Rec>(400, 1000);

    Shard *shards[2];

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    shards[0] = &shard1;
    shards[1] = &shard2;

    auto merged = Shard(shards, 2);

    for (size_t i=100; i<1000; i++) {
        Rec r;
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
}
END_TEST

static void inject_rangequery_tests(Suite *suite) {
    TCase *range_query = tcase_create("Range Query Testing"); 
    tcase_add_test(range_query, t_range_query); 
    tcase_add_test(range_query, t_buffer_range_query); 
    tcase_add_test(range_query, t_range_query_merge); 
    suite_add_tcase(suite, range_query);
}
