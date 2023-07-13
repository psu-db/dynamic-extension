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
    size_t n = 30;

    auto buffer = create_2d_sequential_mbuffer(n);
    auto wss = Shard(buffer);

    for (size_t i=0; i<n; i++) {
        PRec r;
        auto rec = (buffer->get_data() + i);
        r.x = rec->rec.x;
        r.y = rec->rec.y;

        fprintf(stderr, "%ld\n", i);

        auto result = wss.point_lookup(r);
        ck_assert_ptr_nonnull(result);
        ck_assert_int_eq(result->rec.x, r.x);
        ck_assert_int_eq(result->rec.y, r.y);
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
        r.x = i;
        r.y = i;

        auto result = wss.point_lookup(r);
        ck_assert_ptr_null(result);
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


    /*
    TCase *sampling = tcase_create("de:VPTree::VPTreeQuery Testing");
    tcase_add_test(sampling, t_wss_query);
    tcase_add_test(sampling, t_wss_query_merge);
    tcase_add_test(sampling, t_wss_buffer_query_rejection);
    tcase_add_test(sampling, t_wss_buffer_query_scan);
    suite_add_tcase(unit, sampling);
    */

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
