/*
 * tests/include/shard_standard.h
 *
 * Standardized unit tests for Shard objects
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 * WARNING: This file must be included in the main unit test set
 *          after the definition of an appropriate Shard and R
 *          type. In particular, R needs to implement the key-value
 *          pair interface. For other types of record, you'll need to
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
/*
#include "shard/ISAMTree.h"
#include "shard/ISAMTree.h"
#include "testing.h"
#include <check.h>
using namespace de;
typedef Rec R;
typedef ISAMTree<R> Shard;
*/

START_TEST(t_mbuffer_init)
{
    auto buffer = new MutableBuffer<R>(512, 1024);
    for (uint64_t i = 512; i > 0; i--) {
        uint32_t v = i;
        buffer->append({i, v, 1});
    }
    
    for (uint64_t i = 1; i <= 256; ++i) {
        uint32_t v = i;
        buffer->append({i, v, 1}, true);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        uint32_t v = i + 1;
        buffer->append({i, v, 1});
    }

    Shard* shard = new Shard(buffer->get_buffer_view());
    ck_assert_uint_eq(shard->get_record_count(), 512);

    delete buffer;
    delete shard;
}


START_TEST(t_shard_init)
{
    size_t n = 512;
    auto mbuffer1 = create_test_mbuffer<R>(n);
    auto mbuffer2 = create_test_mbuffer<R>(n);
    auto mbuffer3 = create_test_mbuffer<R>(n);

    auto shard1 = new Shard(mbuffer1->get_buffer_view());
    auto shard2 = new Shard(mbuffer2->get_buffer_view());
    auto shard3 = new Shard(mbuffer3->get_buffer_view());

    std::vector<Shard*> shards = {shard1, shard2, shard3};
    auto shard4 = new Shard(shards);

    ck_assert_int_eq(shard4->get_record_count(), n * 3);
    ck_assert_int_eq(shard4->get_tombstone_count(), 0);

    size_t total_cnt = 0;
    size_t shard1_idx = 0;
    size_t shard2_idx = 0;
    size_t shard3_idx = 0;

    for (size_t i = 0; i < shard4->get_record_count(); ++i) {
        auto rec1 = shard1->get_record_at(shard1_idx);
        auto rec2 = shard2->get_record_at(shard2_idx);
        auto rec3 = shard3->get_record_at(shard3_idx);

        auto cur_rec = shard4->get_record_at(i);

        if (shard1_idx < n && cur_rec->rec == rec1->rec) {
            ++shard1_idx;
        } else if (shard2_idx < n && cur_rec->rec == rec2->rec) {
            ++shard2_idx;
        } else if (shard3_idx < n && cur_rec->rec == rec3->rec) {
            ++shard3_idx;
        } else {
           assert(false);
        }
    }

    delete mbuffer1;
    delete mbuffer2;
    delete mbuffer3;

    delete shard1;
    delete shard2;
    delete shard3;
    delete shard4;
}


START_TEST(t_full_cancelation)
{
    size_t n = 100;
    auto buffer = create_double_seq_mbuffer<R>(n, false);
    auto buffer_ts = create_double_seq_mbuffer<R>(n, true);

    Shard* shard = new Shard(buffer->get_buffer_view());
    Shard* shard_ts = new Shard(buffer_ts->get_buffer_view());

    ck_assert_int_eq(shard->get_record_count(), n);
    ck_assert_int_eq(shard->get_tombstone_count(), 0);
    ck_assert_int_eq(shard_ts->get_record_count(), n);
    ck_assert_int_eq(shard_ts->get_tombstone_count(), n);

    std::vector<Shard *> shards = {shard, shard_ts};

    Shard* merged = new Shard(shards);

    ck_assert_int_eq(merged->get_tombstone_count(), 0);
    ck_assert_int_eq(merged->get_record_count(), 0);

    delete buffer;
    delete buffer_ts;
    delete shard;
    delete shard_ts;
    delete merged;
}
END_TEST


START_TEST(t_point_lookup) 
{
    size_t n = 10000;

    auto buffer = create_double_seq_mbuffer<R>(n, false);
    auto isam = Shard(buffer->get_buffer_view());

    {
        auto view = buffer->get_buffer_view();

        for (size_t i=0; i<n; i++) {
            R r;
            auto rec = view.get(i);
            r.key = rec->rec.key;
            r.value = rec->rec.value;

            auto result = isam.point_lookup(r);
            ck_assert_ptr_nonnull(result);
            ck_assert_int_eq(result->rec.key, r.key);
            ck_assert_int_eq(result->rec.value, r.value);
        }
    }

    delete buffer;
}
END_TEST


START_TEST(t_point_lookup_miss) 
{
    size_t n = 10000;

    auto buffer = create_double_seq_mbuffer<R>(n, false);
    auto isam = Shard(buffer->get_buffer_view());

    for (size_t i=n + 100; i<2*n; i++) {
        R r;
        r.key = i;
        r.value = i;

        auto result = isam.point_lookup(r);
        ck_assert_ptr_null(result);
    }

    delete buffer;
}

static void inject_shard_tests(Suite *suite) {
    TCase *create = tcase_create("Shard constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_shard_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(suite, create);
    TCase *tombstone = tcase_create("Shard tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(suite, tombstone); 
    TCase *pointlookup = tcase_create("Shard point lookup Testing"); 
    tcase_add_test(pointlookup, t_point_lookup);
    tcase_add_test(pointlookup, t_point_lookup_miss); 
    suite_add_tcase(suite, pointlookup);
}
