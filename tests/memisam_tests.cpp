/*
 * tests/isam_tests.cpp
 *
 * Unit tests for ISAM Tree shard
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */

#include "shard/ISAMTree.h"
#include "query/irs.h"
#include "testing.h"

#include <check.h>

using namespace de;

typedef ISAMTree<Rec> Shard;

START_TEST(t_mbuffer_init)
{
    auto buffer = new MutableBuffer<Rec>(1024, 1024);
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


START_TEST(t_irs_init)
{
    size_t n = 512;
    auto mbuffer1 = create_test_mbuffer<Rec>(n);
    auto mbuffer2 = create_test_mbuffer<Rec>(n);
    auto mbuffer3 = create_test_mbuffer<Rec>(n);

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

    auto buffer = create_double_seq_mbuffer<Rec>(n, false);
    auto isam = Shard(buffer);

    for (size_t i=0; i<n; i++) {
        Rec r;
        auto rec = (buffer->get_data() + i);
        r.key = rec->rec.key;
        r.value = rec->rec.value;

        auto result = isam.point_lookup(r);
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

    auto buffer = create_double_seq_mbuffer<Rec>(n, false);
    auto isam = Shard(buffer);

    for (size_t i=n + 100; i<2*n; i++) {
        Rec r;
        r.key = i;
        r.value = i;

        auto result = isam.point_lookup(r);
        ck_assert_ptr_null(result);
    }

    delete buffer;
}


START_TEST(t_full_cancelation)
{
    size_t n = 100;
    auto buffer = create_double_seq_mbuffer<Rec>(n, false);
    auto buffer_ts = create_double_seq_mbuffer<Rec>(n, true);

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


START_TEST(t_irs_query)
{
    size_t n=1000;
    auto buffer = create_double_seq_mbuffer<Rec>(n);
    auto isam = Shard(buffer);

    uint64_t lower_key = 100;
    uint64_t upper_key = 250;

    size_t k = 100;

    size_t cnt[3] = {0};
    irs::Parms<Rec> parms = {lower_key, upper_key, k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t total_samples = 0;

    for (size_t i=0; i<1000; i++) {
        auto state = irs::Query<Shard, Rec, false>::get_query_state(&isam, &parms);
        ((irs::State<WRec> *) state)->sample_size = k;
        auto result = irs::Query<Shard, Rec, false>::query(&isam, state, &parms);

        ck_assert_int_eq(result.size(), k);

        for (auto &rec : result) {
            ck_assert_int_le(rec.rec.key, upper_key);
            ck_assert_int_ge(rec.rec.key, lower_key);
        }

        irs::Query<Shard, Rec, false>::delete_query_state(state);
    }

    gsl_rng_free(parms.rng);
    delete buffer;
}
END_TEST


START_TEST(t_irs_query_merge)
{
    size_t n=1000;
    auto buffer = create_double_seq_mbuffer<Rec>(n);

    Shard shard = Shard(buffer);

    uint64_t lower_key = 100;
    uint64_t upper_key = 250;

    size_t k = 1000;

    size_t cnt[3] = {0};
    irs::Parms<Rec> parms = {lower_key, upper_key, k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    std::vector<std::vector<de::Wrapped<Rec>>> results(2);

    for (size_t i=0; i<1000; i++) {
        auto state1 = irs::Query<Shard, Rec>::get_query_state(&shard, &parms);
        ((irs::State<WRec> *) state1)->sample_size = k;
        results[0] = irs::Query<Shard, Rec>::query(&shard, state1, &parms);

        auto state2 = irs::Query<Shard, Rec>::get_query_state(&shard, &parms);
        ((irs::State<WRec> *) state2)->sample_size = k;
        results[1] = irs::Query<Shard, Rec>::query(&shard, state2, &parms);

        irs::Query<Shard, Rec>::delete_query_state(state1);
        irs::Query<Shard, Rec>::delete_query_state(state2);
    }

    auto merged = irs::Query<Shard, Rec>::merge(results, nullptr);

    ck_assert_int_eq(merged.size(), 2*k);
    for (size_t i=0; i<merged.size(); i++) {
        ck_assert_int_ge(merged[i].key, lower_key);
        ck_assert_int_le(merged[i].key, upper_key);
    }

    gsl_rng_free(parms.rng);
    delete buffer;
}
END_TEST


START_TEST(t_irs_buffer_query_scan)
{
    size_t n=1000;
    auto buffer = create_double_seq_mbuffer<Rec>(n);

    uint64_t lower_key = 100;
    uint64_t upper_key = 250;

    size_t k = 100;

    size_t cnt[3] = {0};
    irs::Parms<Rec> parms = {lower_key, upper_key, k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t total_samples = 0;

    for (size_t i=0; i<1000; i++) {
        auto state = irs::Query<Shard, Rec, false>::get_buffer_query_state(buffer, &parms);
        ((irs::BufferState<WRec> *) state)->sample_size = k;
        auto result = irs::Query<Shard, Rec, false>::buffer_query(buffer, state, &parms);

        ck_assert_int_eq(result.size(), k);

        for (auto &rec : result) {
            ck_assert_int_le(rec.rec.key, upper_key);
            ck_assert_int_ge(rec.rec.key, lower_key);
        }

        irs::Query<Shard, Rec, false>::delete_buffer_query_state(state);
    }

    gsl_rng_free(parms.rng);
    delete buffer;
}
END_TEST


START_TEST(t_irs_buffer_query_rejection)
{
    size_t n=1000;
    auto buffer = create_double_seq_mbuffer<Rec>(n);

    uint64_t lower_key = 100;
    uint64_t upper_key = 250;

    size_t k = 10000;

    size_t cnt[3] = {0};
    irs::Parms<Rec> parms = {lower_key, upper_key, k};
    parms.rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t total_samples = 0;

    for (size_t i=0; i<1000; i++) {
        auto state = irs::Query<Shard, Rec>::get_buffer_query_state(buffer, &parms);
        ((irs::BufferState<WRec> *) state)->sample_size = k;
        auto result = irs::Query<Shard, Rec>::buffer_query(buffer, state, &parms);

        ck_assert_int_gt(result.size(), 0);
        ck_assert_int_le(result.size(), k);

        for (auto &rec : result) {
            ck_assert_int_le(rec.rec.key, upper_key);
            ck_assert_int_ge(rec.rec.key, lower_key);
        }

        irs::Query<Shard, Rec>::delete_buffer_query_state(state);
    }

    gsl_rng_free(parms.rng);
    delete buffer;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("ISAMTree Shard Unit Testing");

    TCase *create = tcase_create("de::ISAMTree constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_irs_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);


    TCase *tombstone = tcase_create("de:ISAMTree::tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(unit, tombstone);


    TCase *lookup = tcase_create("de:ISAMTree:point_lookup Testing");
    tcase_add_test(lookup, t_point_lookup);
    tcase_add_test(lookup, t_point_lookup_miss);
    suite_add_tcase(unit, lookup);


    TCase *sampling = tcase_create("de:ISAMTree::ISAMTreeQuery Testing");
    tcase_add_test(sampling, t_irs_query);
    tcase_add_test(sampling, t_irs_query_merge);
    tcase_add_test(sampling, t_irs_buffer_query_rejection);
    tcase_add_test(sampling, t_irs_buffer_query_scan);
    tcase_set_timeout(sampling, 100);
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
