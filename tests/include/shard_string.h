/*
 * tests/include/shard_string.h
 *
 * Standardized unit tests for Shard objects with string keys
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
#include "shard/FSTrie.h"
#include "testing.h"
#include <check.h>
using namespace de;
//typedef StringRec R;
//typedef FSTrie<R> Shard;

START_TEST(t_mbuffer_init)
{

    auto recs = read_string_data(kjv_wordlist, 1024);

    auto buffer = new MutableBuffer<R>(512, 1024);

    for (uint64_t i = 0; i < 512; i++) {
        buffer->append(recs[i]);
    }
    
    for (uint64_t i = 0; i < 256; ++i) {
        buffer->delete_record(recs[i]);
    }

    for (uint64_t i = 512; i < 768; ++i) {
        buffer->append(recs[i]);
    }

    Shard* shard = new Shard(buffer->get_buffer_view());
    ck_assert_uint_eq(shard->get_record_count(), 512);

    delete buffer;
    delete shard;
}


START_TEST(t_shard_init)
{
    size_t n = 2048;
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

START_TEST(t_point_lookup) 
{
    size_t n = 10000;

    auto buffer = create_test_mbuffer<R>(n);
    auto shard = Shard(buffer->get_buffer_view());

    {
        auto view = buffer->get_buffer_view();

        for (size_t i=0; i<n; i++) {
            auto rec = view.get(i);
            R r = rec->rec;

            auto result = shard.point_lookup(r);
            ck_assert_ptr_nonnull(result);
            //ck_assert_str_eq(result->rec.key, r.key);
            //ck_assert_int_eq(result->rec.value, r.value);
            //fprintf(stderr, "%ld\n", i);
        }
    }

    delete buffer;
}
END_TEST


START_TEST(t_point_lookup_miss) 
{
    size_t n = 10000;

    auto buffer = create_test_mbuffer<R>(n);
    auto shard = Shard(buffer->get_buffer_view());

    for (size_t i=n + 100; i<2*n; i++) {
        const char *c = "computer";
        R r = {c, 1234, 8};

        auto result = shard.point_lookup(r);
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

    TCase *pointlookup = tcase_create("Shard point lookup Testing"); 
    tcase_add_test(pointlookup, t_point_lookup);
    tcase_add_test(pointlookup, t_point_lookup_miss); 
    suite_add_tcase(suite, pointlookup);
}
