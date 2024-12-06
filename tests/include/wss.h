/*
 * tests/include/wss.h
 *
 * Standardized unit tests for weighted set sampling against supporting
 * shard types
 *
 * Copyright (C) 2023-2024 Douglas Rumbaugh <drumbaugh@psu.edu> 
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

#include "query/wss.h"

/*
 * Uncomment these lines temporarily to remove errors in this file
 * temporarily for development purposes. They should be removed prior
 * to building, to ensure no duplicate definitions. These includes/defines
 * should be included in the source file that includes this one, above the
 * include statement.
 */
// #include "framework/interface/Record.h"
// #include "shard/Alias.h"
// #include "testing.h"
// #include <check.h>

// using namespace de;

// typedef WeightedRecord<int64_t, int32_t, int32_t> R;
// typedef Alias<R> Shard;

typedef wss::Query<Shard> Q;

START_TEST(t_wss_query)
{
    auto buffer = create_weighted_mbuffer<R>(1000);
    auto shard = Shard(buffer->get_buffer_view());
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t k = 20;
    
    Q::Parameters parms;
    parms.rng = rng;
    parms.sample_size = k;

    auto query = Q::local_preproc(&shard, &parms);
    Q::distribute_query(&parms, {query}, nullptr);

    auto result = Q::local_query(&shard, query);
    delete query;

    ck_assert_int_eq(result.size(), k);


    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_buffer_wss_query)
{
    auto buffer = create_weighted_mbuffer<R>(1000);
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t k = 20;
    
    Q::Parameters parms;
    parms.rng = rng;
    parms.sample_size = k;

    {
        auto view = buffer->get_buffer_view();
        auto query = Q::local_preproc_buffer(&view, &parms);
        Q::distribute_query(&parms, {}, query);
        auto result = Q::local_query_buffer(query);

        delete query;
        ck_assert_int_le(result.size(), k);
    }

    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


static void inject_wss_tests(Suite *suite) {
    TCase *wss_query = tcase_create("WSS Query Testing"); 
    tcase_add_test(wss_query, t_wss_query); 
    tcase_add_test(wss_query, t_buffer_wss_query); 
    //tcase_add_test(wss_query, t_wss_query_merge); 
    suite_add_tcase(suite, wss_query);
}
