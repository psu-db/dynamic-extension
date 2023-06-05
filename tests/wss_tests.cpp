/*
 * tests/wss_tests.cpp
 *
 * Unit tests for WSS (Augmented B+Tree) shard
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */

#include "shard/WSS.h"
#include "testing.h"

#include <check.h>

using namespace de;

typedef WSS<WRec> Shard;

START_TEST(t_mbuffer_init)
{
    auto buffer = new MutableBuffer<WRec>(1024, true, 1024);
    for (uint64_t i = 512; i > 0; i--) {
        uint32_t v = i;
        buffer->append({i,v, 1});
    }
    
    for (uint64_t i = 1; i <= 256; ++i) {
        uint32_t v = i;
        buffer->append({i, v, 1}, true);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        uint32_t v = i + 1;
        buffer->append({i, v, 1});
    }

    Shard* shard = new Shard(buffer);
    ck_assert_uint_eq(shard->get_record_count(), 512);

    delete buffer;
    delete shard;
}


START_TEST(t_wss_init)
{
    size_t n = 512;
    auto mbuffer1 = create_test_mbuffer<WRec>(n);
    auto mbuffer2 = create_test_mbuffer<WRec>(n);
    auto mbuffer3 = create_test_mbuffer<WRec>(n);

    auto shard1 = new Shard(mbuffer1);
    auto shard2 = new Shard(mbuffer2);
    auto shard3 = new Shard(mbuffer3);

    Shard* shards[3] = {shard1, shard2, shard3};
    auto shard4 = new Shard(shards, 3);

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


START_TEST(t_point_lookup) 
{
    size_t n = 10000;

    auto buffer = create_double_seq_mbuffer<WRec>(n, false);
    auto wss = Shard(buffer);

    for (size_t i=0; i<n; i++) {
        WRec r;
        auto rec = (buffer->get_data() + i);
        r.key = rec->rec.key;
        r.value = rec->rec.value;

        auto result = wss.point_lookup(r);
        ck_assert_ptr_nonnull(result);
        ck_assert_int_eq(result->rec.key, r.key);
        ck_assert_int_eq(result->rec.value, r.value);
    }

    delete buffer;
}
END_TEST


START_TEST(t_point_lookup_miss) 
{
    size_t n = 10000;

    auto buffer = create_double_seq_mbuffer<WRec>(n, false);
    auto wss = Shard(buffer);

    for (size_t i=n + 100; i<2*n; i++) {
        WRec r;
        r.key = i;
        r.value = i;

        auto result = wss.point_lookup(r);
        ck_assert_ptr_null(result);
    }

    delete buffer;
}

START_TEST(t_full_cancelation)
{
    size_t n = 100;
    auto buffer = create_double_seq_mbuffer<WRec>(n, false);
    auto buffer_ts = create_double_seq_mbuffer<WRec>(n, true);

    Shard* shard = new Shard(buffer);
    Shard* shard_ts = new Shard(buffer_ts);

    ck_assert_int_eq(shard->get_record_count(), n);
    ck_assert_int_eq(shard->get_tombstone_count(), 0);
    ck_assert_int_eq(shard_ts->get_record_count(), n);
    ck_assert_int_eq(shard_ts->get_tombstone_count(), n);

    Shard* shards[] = {shard, shard_ts};

    Shard* merged = new Shard(shards, 2);

    ck_assert_int_eq(merged->get_tombstone_count(), 0);
    ck_assert_int_eq(merged->get_record_count(), 0);

    delete buffer;
    delete buffer_ts;
    delete shard;
    delete shard_ts;
    delete merged;
}
END_TEST


START_TEST(t_wss_query)
{
    size_t n=1000;
    auto buffer = create_weighted_mbuffer<WRec>(n);

    Shard* shard = new Shard(buffer);

    size_t k = 1000;

    size_t cnt[3] = {0};
    wss_query_parms<WRec> parms = {k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t total_samples = 0;

    for (size_t i=0; i<1000; i++) {
        auto state = WSSQuery<WRec>::get_query_state(shard, &parms);
        auto result = WSSQuery<WRec>::query(shard, state, &parms);

        total_samples += result.size();

        for (size_t j=0; j<result.size(); j++) {
            cnt[result[j].rec.key - 1]++;
        }

        WSSQuery<WRec>::delete_query_state(state);
    }

    ck_assert(roughly_equal(cnt[0], (double) total_samples/4.0, total_samples, .05));
    ck_assert(roughly_equal(cnt[1], (double) total_samples/4.0, total_samples, .05));
    ck_assert(roughly_equal(cnt[2], (double) total_samples/2.0, total_samples, .05));

    gsl_rng_free(parms.rng);
    delete shard;
    delete buffer;
}
END_TEST


START_TEST(t_wss_query_merge)
{
    size_t n=1000;
    auto buffer = create_weighted_mbuffer<WRec>(n);

    Shard* shard = new Shard(buffer);

    uint64_t lower_key = 0;
    uint64_t upper_key = 5;

    size_t k = 1000;

    size_t cnt[3] = {0};
    wss_query_parms<WRec> parms = {k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    std::vector<std::vector<WRec>> results(2);

    for (size_t i=0; i<1000; i++) {
        auto state1 = WSSQuery<WRec>::get_query_state(shard, &parms);
        results[0] = strip_wrapping(WSSQuery<WRec>::query(shard, state1, &parms));

        auto state2 = WSSQuery<WRec>::get_query_state(shard, &parms);
        results[1] = strip_wrapping(WSSQuery<WRec>::query(shard, state2, &parms));

        WSSQuery<WRec>::delete_query_state(state1);
        WSSQuery<WRec>::delete_query_state(state2);
    }

    auto merged = WSSQuery<WRec>::merge(results);

    ck_assert_int_eq(merged.size(), 2*k);
    for (size_t i=0; i<merged.size(); i++) {
        ck_assert_int_ge(merged[i].key, lower_key);
        ck_assert_int_le(merged[i].key, upper_key);
    }

    gsl_rng_free(parms.rng);
    delete shard;
    delete buffer;
}
END_TEST


START_TEST(t_wss_buffer_query_scan)
{
    size_t n=1000;
    auto buffer = create_weighted_mbuffer<WRec>(n);

    uint64_t lower_key = 0;
    uint64_t upper_key = 5;

    size_t k = 1000;

    size_t cnt[3] = {0};
    wss_query_parms<WRec> parms = {k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t total_samples = 0;

    for (size_t i=0; i<1000; i++) {
        auto state = WSSQuery<WRec, false>::get_buffer_query_state(buffer, &parms);
        auto result = WSSQuery<WRec, false>::buffer_query(buffer, state, &parms);
        total_samples += result.size();

        for (size_t j=0; j<result.size(); j++) {
            cnt[result[j].rec.key - 1]++;
        }

        WSSQuery<WRec, false>::delete_buffer_query_state(state);
    }

    ck_assert(roughly_equal(cnt[0], (double) total_samples/4.0, total_samples, .05));
    ck_assert(roughly_equal(cnt[1], (double) total_samples/4.0, total_samples, .05));
    ck_assert(roughly_equal(cnt[2], (double) total_samples/2.0, total_samples, .05));

    gsl_rng_free(parms.rng);
    delete buffer;
}
END_TEST


START_TEST(t_wss_buffer_query_rejection)
{
    size_t n=1000;
    auto buffer = create_weighted_mbuffer<WRec>(n);

    uint64_t lower_key = 0;
    uint64_t upper_key = 5;

    size_t k = 1000;

    size_t cnt[3] = {0};
    wss_query_parms<WRec> parms = {k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t total_samples = 0;

    for (size_t i=0; i<1000; i++) {
        auto state = WSSQuery<WRec>::get_buffer_query_state(buffer, &parms);
        auto result = WSSQuery<WRec>::buffer_query(buffer, state, &parms);

        total_samples += result.size();

        for (size_t j=0; j<result.size(); j++) {
            cnt[result[j].rec.key - 1]++;
        }

        WSSQuery<WRec>::delete_buffer_query_state(state);
    }

    ck_assert(roughly_equal(cnt[0], (double) total_samples/4.0, total_samples, .1));
    ck_assert(roughly_equal(cnt[1], (double) total_samples/4.0, total_samples, .1));
    ck_assert(roughly_equal(cnt[2], (double) total_samples/2.0, total_samples, .1));

    gsl_rng_free(parms.rng);
    delete buffer;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("WSS Shard Unit Testing");

    TCase *create = tcase_create("de::WSS constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_wss_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);


    TCase *tombstone = tcase_create("de:WSS::tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(unit, tombstone);


    TCase *lookup = tcase_create("de:WSS:point_lookup Testing");
    tcase_add_test(lookup, t_point_lookup);
    tcase_add_test(lookup, t_point_lookup_miss);
    suite_add_tcase(unit, lookup);



    TCase *sampling = tcase_create("de:WSS::WSSQuery Testing");
    tcase_add_test(sampling, t_wss_query);
    tcase_add_test(sampling, t_wss_query_merge);
    tcase_add_test(sampling, t_wss_buffer_query_rejection);
    tcase_add_test(sampling, t_wss_buffer_query_scan);
    suite_add_tcase(unit, sampling);

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
