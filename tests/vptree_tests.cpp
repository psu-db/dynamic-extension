/*
 * tests/vptree_tests.cpp
 *
 * Unit tests for VPTree (knn queries)
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */

#include "shard/VPTree.h"
#include "testing.h"
#include "vptree.hpp"

#include <check.h>

using namespace de;


typedef VPTree<PRec> Shard;

START_TEST(t_mbuffer_init)
{
    size_t n= 24;
    auto buffer = new MutableBuffer<PRec>(n, n);

    for (int64_t i=0; i<n; i++) {
        buffer->append({i, i});
    }

    Shard* shard = new Shard(buffer);
    ck_assert_uint_eq(shard->get_record_count(), n);

    delete buffer;
    delete shard;
}


START_TEST(t_wss_init)
{
    size_t n = 512;
    auto mbuffer1 = create_2d_mbuffer(n);
    auto mbuffer2 = create_2d_mbuffer(n);
    auto mbuffer3 = create_2d_mbuffer(n);

    auto shard1 = new Shard(mbuffer1);
    auto shard2 = new Shard(mbuffer2);
    auto shard3 = new Shard(mbuffer3);

    Shard* shards[3] = {shard1, shard2, shard3};
    auto shard4 = new Shard(shards, 3);

    ck_assert_int_eq(shard4->get_record_count(), n * 3);
    ck_assert_int_eq(shard4->get_tombstone_count(), 0);

    delete mbuffer1;
    delete mbuffer2;
    delete mbuffer3;

    delete shard1;
    delete shard2;
    delete shard3;
    delete shard4;
}


START_TEST(t_point_lookup) 
{
    size_t n = 16;

    auto buffer = create_2d_sequential_mbuffer(n);
    auto wss = Shard(buffer);

    for (size_t i=0; i<n; i++) {
        PRec r;
        auto rec = (buffer->get_data() + i);
        r.data[0] = rec->rec.data[0];
        r.data[1] = rec->rec.data[1];

        auto result = wss.point_lookup(r);
        ck_assert_ptr_nonnull(result);
        ck_assert_int_eq(result->rec.data[0], r.data[0]);
        ck_assert_int_eq(result->rec.data[1], r.data[1]);
    }

    delete buffer;
}
END_TEST


START_TEST(t_point_lookup_miss) 
{
    size_t n = 10000;

    auto buffer = create_2d_sequential_mbuffer(n);
    auto wss = Shard(buffer);

    for (size_t i=n + 100; i<2*n; i++) {
        PRec r;
        r.data[0] = i;
        r.data[1] = i;

        auto result = wss.point_lookup(r);
        ck_assert_ptr_null(result);
    }

    delete buffer;
}


START_TEST(t_buffer_query) 
{
    size_t n = 10000;
    auto buffer = create_2d_sequential_mbuffer(n);

    PRec target;
    target.data[0] = 120;
    target.data[1] = 120;

    KNNQueryParms<PRec> p;
    p.k = 10;
    p.point = target;

    auto state = KNNQuery<PRec>::get_buffer_query_state(buffer, &p);
    auto result = KNNQuery<PRec>::buffer_query(buffer, state, &p);
    KNNQuery<PRec>::delete_buffer_query_state(state);

    std::sort(result.begin(), result.end());
    size_t start = 120 - 5;
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_eq(result[i].rec.data[0], start++);
    }

    delete buffer;
}

START_TEST(t_knn_query) 
{
    size_t n = 1000;
    auto buffer = create_2d_sequential_mbuffer(n);

    auto vptree = VPTree<PRec>(buffer);

    KNNQueryParms<PRec> p;
    for (size_t i=0; i<100; i++) {
        p.k = rand() % 150;
        p.point.data[0] = rand() % (n-p.k);
        p.point.data[1] = p.point.data[0];

        auto state = KNNQuery<PRec>::get_query_state(&vptree, &p);
        auto results = KNNQuery<PRec>::query(&vptree, state, &p);
        KNNQuery<PRec>::delete_query_state(state);

        ck_assert_int_eq(results.size(), p.k);

        std::sort(results.begin(), results.end());

        if ((int64_t) (p.point.data[0] - p.k/2 - 1) < 0) {
            ck_assert_int_eq(results[0].rec.data[0], 0);
        } else {
            ck_assert(results[0].rec.data[0] == (p.point.data[0] - p.k/2 - 1) ||
                      results[0].rec.data[0] == (p.point.data[0] - p.k/2) ||
                      results[0].rec.data[0] == (p.point.data[0] - p.k/2 + 1));
        }


        size_t start = results[0].rec.data[0];
        for (size_t i=0; i<results.size(); i++) {
            ck_assert_int_eq(results[i].rec.data[0], start++);
        }
    }

    delete buffer;
}


Suite *unit_testing()
{
    Suite *unit = suite_create("VPTree Shard Unit Testing");

    TCase *create = tcase_create("de::VPTree constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_wss_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);


    TCase *lookup = tcase_create("de:VPTree:point_lookup Testing");
    tcase_add_test(lookup, t_point_lookup);
    tcase_add_test(lookup, t_point_lookup_miss);
    suite_add_tcase(unit, lookup);


    TCase *query = tcase_create("de:VPTree::VPTreeQuery Testing");
    tcase_add_test(query, t_buffer_query);
    tcase_add_test(query, t_knn_query);
    suite_add_tcase(unit, query);

    return unit;
}


int shard_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_shardner = srunner_create(unit);

    srunner_run_all(unit_shardner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_shardner);
    srunner_free(unit_shardner);

    return failed;
}


int main() 
{
    int unit_failed = shard_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
