/*
 * tests/vptree_tests.cpp
 *
 * Unit tests for VPTree (knn queries)
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */


#include "include/testing.h"
#include "shard/VPTree.h"
#include "query/knn.h"

#include <check.h>

using namespace de;

typedef PRec R;
typedef VPTree<R> Shard;

START_TEST(t_mbuffer_init)
{
    size_t n= 24;
    auto buffer = new MutableBuffer<PRec>(n/2, n);

    for (int64_t i=0; i<n; i++) {
        buffer->append({(uint64_t) i, (uint64_t) i});
    }

    Shard* shard = new Shard(buffer->get_buffer_view());
    ck_assert_uint_eq(shard->get_record_count(), n);

    delete buffer;
    delete shard;
}


START_TEST(t_wss_init)
{
    size_t n = 512;
    auto mbuffer1 = create_test_mbuffer<R>(n);
    auto mbuffer2 = create_test_mbuffer<R>(n);
    auto mbuffer3 = create_test_mbuffer<R>(n);

    auto shard1 = new Shard(mbuffer1->get_buffer_view());
    auto shard2 = new Shard(mbuffer2->get_buffer_view());
    auto shard3 = new Shard(mbuffer3->get_buffer_view());

    std::vector<Shard *> shards = {shard1, shard2, shard3};
    auto shard4 = new Shard(shards);

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

    auto buffer = create_sequential_mbuffer<R>(0, n);
    auto wss = Shard(buffer->get_buffer_view());

    {
        auto bv = buffer->get_buffer_view();

        for (size_t i=0; i<n; i++) {
            PRec r;
            auto rec = (bv.get(i));
            r.data[0] = rec->rec.data[0];
            r.data[1] = rec->rec.data[1];

            auto result = wss.point_lookup(r);
            ck_assert_ptr_nonnull(result);
            ck_assert_int_eq(result->rec.data[0], r.data[0]);
            ck_assert_int_eq(result->rec.data[1], r.data[1]);
        }
    }

    delete buffer;
}
END_TEST


START_TEST(t_point_lookup_miss) 
{
    size_t n = 10000;

    auto buffer = create_sequential_mbuffer<R>(0, n);
    auto wss = Shard(buffer->get_buffer_view());

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
    auto buffer = create_sequential_mbuffer<R>(0, n);

    PRec target;
    target.data[0] = 120;
    target.data[1] = 120;

    knn::Parms<PRec> p;
    p.k = 10;
    p.point = target;

    {
        auto bv = buffer->get_buffer_view();
        auto state = knn::Query<PRec, Shard>::get_buffer_query_state(&bv, &p);
        auto result = knn::Query<PRec, Shard>::buffer_query(state, &p);
        knn::Query<PRec, Shard>::delete_buffer_query_state(state);

        std::sort(result.begin(), result.end());
        size_t start = 120 - 5;
        for (size_t i=0; i<result.size(); i++) {
            ck_assert_int_eq(result[i].rec.data[0], start++);
        }
    }

    delete buffer;
}

START_TEST(t_knn_query) 
{
    size_t n = 1000;
    auto buffer = create_sequential_mbuffer<R>(0, n);

    auto vptree = VPTree<PRec>(buffer->get_buffer_view());

    knn::Parms<PRec> p;
    for (size_t i=0; i<100; i++) {
        p.k = rand() % 150;
        p.point.data[0] = rand() % (n-p.k);
        p.point.data[1] = p.point.data[0];

        auto state = knn::Query<PRec, Shard>::get_query_state(&vptree, &p);
        auto results = knn::Query<PRec, Shard>::query(&vptree, state, &p);
        knn::Query<PRec, Shard>::delete_query_state(state);

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
