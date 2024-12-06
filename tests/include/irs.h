/*
 * tests/include/irs.h
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

#include "query/irs.h"
#include <algorithm>

/*
 * Uncomment these lines temporarily to remove errors in this file
 * temporarily for development purposes. They should be removed prior
 * to building, to ensure no duplicate definitions. These includes/defines
 * should be included in the source file that includes this one, above the
 * include statement.
 */
#include "shard/ISAMTree.h"
#include "query/irs.h"
#include "testing.h"
#include <check.h>
#include <gsl/gsl_rng.h>
using namespace de;

typedef Rec R;
typedef ISAMTree<R> Shard;
typedef irs::Query<ISAMTree<R>> Query;

static gsl_rng *g_rng;

START_TEST(t_irs)
{
    auto buffer = create_sequential_mbuffer<R>(100, 1000);
    auto shard = Shard(buffer->get_buffer_view());

    size_t k = 5;
    irs::Query<Shard>::Parameters parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;
    parms.sample_size = k;
    parms.rng = g_rng;

    auto local_query = irs::Query<Shard>::local_preproc(&shard, &parms);
    irs::Query<Shard>::distribute_query(&parms, {local_query}, nullptr);

    auto result = irs::Query<Shard>::local_query(&shard, local_query);
    delete local_query;

    ck_assert_int_eq(result.size(), k);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_buffer_irs)
{
    auto buffer = create_sequential_mbuffer<R>(100, 1000);

    size_t k = 5;
    irs::Query<Shard>::Parameters parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;
    parms.sample_size = k;
    parms.rng = g_rng;

    {
        auto view = buffer->get_buffer_view();
        auto query = irs::Query<Shard>::local_preproc_buffer(&view, &parms);
        irs::Query<Shard>::distribute_query(&parms, {}, query);
        auto result = irs::Query<Shard>::local_query_buffer(query); 
        delete query;

        ck_assert_int_le(result.size(), k);
        for (size_t i=0; i<result.size(); i++) {
            ck_assert_int_le(result[i].rec.key, parms.upper_bound);
            ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
        }
    }

    delete buffer;
}
END_TEST


START_TEST(t_irs_merge)
{    
    auto buffer1 = create_sequential_mbuffer<R>(100, 200);
    auto buffer2 = create_sequential_mbuffer<R>(400, 1000);

    auto shard1 = Shard(buffer1->get_buffer_view());
    auto shard2 = Shard(buffer2->get_buffer_view());

    size_t k = 10;
    irs::Query<Shard>::Parameters parms;
    parms.lower_bound = 150;
    parms.upper_bound = 500;
    parms.sample_size = k;
    parms.rng = g_rng;

    /* necessary to store the alias structure */
    auto dummy_buffer_query = irs::Query<Shard>::LocalQueryBuffer();
    dummy_buffer_query.buffer = nullptr;
    dummy_buffer_query.sample_size = 0;
    dummy_buffer_query.cutoff = 0;
    dummy_buffer_query.global_parms = parms;
    dummy_buffer_query.records = {};
    dummy_buffer_query.alias = nullptr;
    
    auto query1 = irs::Query<Shard>::local_preproc(&shard1, &parms);
    auto query2 = irs::Query<Shard>::local_preproc(&shard2, &parms);

    irs::Query<Shard>::distribute_query(&parms, {query1, query2}, &dummy_buffer_query);

    std::vector<std::vector<irs::Query<Shard>::LocalResultType>> results(2);
    results[0] = irs::Query<Shard>::local_query(&shard1, query1);     
    results[1] = irs::Query<Shard>::local_query(&shard2, query2); 
    delete query1;
    delete query2;

    ck_assert_int_eq(results[0].size() + results[1].size(), k);

    std::vector<std::vector<Wrapped<R>>> proc_results;

    for (size_t j=0; j<results.size(); j++) {
        proc_results.emplace_back(std::vector<Wrapped<R>>());
        for (size_t i=0; i<results[j].size(); i++) {
            proc_results[j].emplace_back(results[j][i]);
        }
    }

    std::vector<irs::Query<Shard>::ResultType> result;
    irs::Query<Shard>::combine(proc_results, nullptr, result);
    ck_assert_int_eq(result.size(), k);

    delete buffer1;
    delete buffer2;
}
END_TEST

static void inject_irs_tests(Suite *suite) {
    g_rng = gsl_rng_alloc(gsl_rng_mt19937);

    TCase *irs = tcase_create("Independent Range Sampling Query Testing"); 
    tcase_add_test(irs, t_irs); 
    tcase_add_test(irs, t_buffer_irs); 
    tcase_add_test(irs, t_irs_merge); 
    suite_add_tcase(suite, irs);
}
