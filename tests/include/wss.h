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

/*
 * Uncomment these lines temporarily to remove errors in this file
 * temporarily for development purposes. They should be removed prior
 * to building, to ensure no duplicate definitions. These includes/defines
 * should be included in the source file that includes this one, above the
 * include statement.
 */
#include "shard/Alias.h"
#include "testing.h"
#include <check.h>
using namespace de;
typedef Alias<R> Shard;

#include "query/wss.h"

START_TEST(t_wss_query)
{
    auto buffer = create_weighted_mbuffer<R>(1000);
    auto shard = Shard(buffer->get_buffer_view());

    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    wss::Parms<R> parms;
    parms.rng = rng;
    parms.sample_size = 20;

    auto state = wss::Query<R, Shard>::get_query_state(&shard, &parms);
    auto result = wss::Query<R, Shard>::query(&shard, state, &parms);
    wss::Query<R, Shard>::delete_query_state(state);

    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_buffer_wss_query)
{
    auto buffer = create_weighted_mbuffer<R>(1000);


    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    wss::Parms<R> parms;
    parms.rng = rng;

    {
        auto view = buffer->get_buffer_view();
        auto state = wss::Query<R, Shard>::get_buffer_query_state(&view, &parms);
        auto result = wss::Query<R, Shard>::buffer_query(state, &parms);
        wss::Query<R, Shard>::delete_buffer_query_state(state);

        ck_assert_int_eq(result.size(), parms.sample_size);
        for (size_t i=0; i<result.size(); i++) {
            
        }
    }

    delete buffer;
}
END_TEST


/*
START_TEST(t_range_query_merge)
{    
    auto buffer1 = create_sequential_mbuffer<R>(100, 200);
    auto buffer2 = create_sequential_mbuffer<R>(400, 1000);

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    wss::Parms<R> parms;
    parms.lower_bound = 150;
    parms.upper_bound = 500;

    size_t result_size = parms.upper_bound - parms.lower_bound + 1 - 200;

    auto state1 = wss::Query<R, Shard>::get_query_state(&shard1, &parms);
    auto state2 = wss::Query<R, Shard>::get_query_state(&shard2, &parms);

    std::vector<std::vector<de::Wrapped<R>>> results(2);
    results[0] = wss::Query<R, Shard>::query(&shard1, state1, &parms);
    results[1] = wss::Query<R, Shard>::query(&shard2, state2, &parms);

    wss::Query<R, Shard>::delete_query_state(state1);
    wss::Query<R, Shard>::delete_query_state(state2);

    ck_assert_int_eq(results[0].size() + results[1].size(), result_size);

    std::vector<std::vector<Wrapped<R>>> proc_results;

    for (size_t j=0; j<results.size(); j++) {
        proc_results.emplace_back(std::vector<Wrapped<R>>());
        for (size_t i=0; i<results[j].size(); i++) {
            proc_results[j].emplace_back(results[j][i]);
        }
    }

    auto result = wss::Query<R, Shard>::merge(proc_results, nullptr);
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
*/


static void inject_wss_tests(Suite *suite) {
    TCase *wss_query = tcase_create("WSS Query Testing"); 
    tcase_add_test(wss_query, t_wss_query); 
    tcase_add_test(wss_query, t_buffer_wss_query); 
    //tcase_add_test(wss_query, t_wss_query_merge); 
    suite_add_tcase(suite, wss_query);
}
