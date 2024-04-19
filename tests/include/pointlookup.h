/*
 * tests/include/pointlookup.h
 *
 * Standardized unit tests for point lookups against supporting
 * shard types (must be unique for the moment)
 *
 * Copyright (C) 2024 Douglas Rumbaugh <drumbaugh@psu.edu> 
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

//#include "shard/FSTrie.h"
#include "query/pointlookup.h"
#include "testing.h"

#include <check.h>

using namespace de;
//typedef StringRec R;
//typedef FSTrie<R> Shard;

START_TEST(t_point_lookup_query)
{
    auto buffer = create_test_mbuffer<R>(1000);
    auto shard = Shard(buffer->get_buffer_view());

    {
        auto bv = buffer->get_buffer_view();
        for (size_t i=0; i<bv.get_record_count(); i++) {
            auto key = bv.get(i)->rec.key;

            pl::Parms<R> parms = {key};
            auto state = pl::Query<R, Shard>::get_query_state(&shard, &parms);
            auto result = pl::Query<R, Shard>::query(&shard, state, &parms);
            pl::Query<R, Shard>::delete_query_state(state);

            ck_assert_int_eq(result.size(), 1);
            //ck_assert_str_eq(result[0].rec.key, key);
            //ck_assert_int_eq(result[0].rec.value, bv.get(i)->rec.value);
        }

        /* point lookup miss; result size should be 0 */
        const char *c = "computer";
        pl::Parms<R> parms = {c};

        auto state = pl::Query<R, Shard>::get_query_state(&shard, &parms);
        auto result = pl::Query<R, Shard>::query(&shard, state, &parms);
        pl::Query<R, Shard>::delete_query_state(state);

        ck_assert_int_eq(result.size(), 0);
    }

    delete buffer;
}
END_TEST


START_TEST(t_buffer_point_lookup)
{

    auto buffer = create_test_mbuffer<R>(1000);
    {
        auto view = buffer->get_buffer_view();
        for (int i=view.get_record_count()-1; i>=0; i--) {
            pl::Parms<R> parms = {view.get(i)->rec.key};

            auto state = pl::Query<R, Shard>::get_buffer_query_state(&view, &parms);
            auto result = pl::Query<R, Shard>::buffer_query(state, &parms);
            pl::Query<R, Shard>::delete_buffer_query_state(state);

            ck_assert_int_eq(result.size(), 1);
            //ck_assert_str_eq(result[0].rec.key, view.get(i)->rec.key);
            //ck_assert_int_eq(result[0].rec.value, view.get(i)->rec.value);
        }

        /* point lookup miss; result size should be 0 */
        const char *c = "computer";
        pl::Parms<R> parms = {c};

        auto state = pl::Query<R, Shard>::get_buffer_query_state(&view, &parms);
        auto result = pl::Query<R, Shard>::buffer_query(state, &parms);
        pl::Query<R, Shard>::delete_buffer_query_state(state);

        ck_assert_int_eq(result.size(), 0);
    }

    delete buffer;
}
END_TEST


static void inject_pointlookup_tests(Suite *suite) {
    TCase *point_lookup_query = tcase_create("Point Lookup Testing"); 
    tcase_add_test(point_lookup_query, t_point_lookup_query); 
    tcase_add_test(point_lookup_query, t_buffer_point_lookup); 
    suite_add_tcase(suite, point_lookup_query);
}
