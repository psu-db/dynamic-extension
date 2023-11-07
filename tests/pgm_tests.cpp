/*
 * tests/irs_tests.cpp
 *
 * Unit tests for PGM (Augmented B+Tree) shard
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */

#include "shard/PGM.h"
#include "query/rangequery.h"
#include "testing.h"

#include <check.h>

using namespace de;

typedef PGM<Rec> Shard;

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
    auto shard = Shard(buffer);

    for (size_t i=0; i<n; i++) {
        Rec r;
        auto rec = (buffer->get_data() + i);
        r.key = rec->rec.key;
        r.value = rec->rec.value;

        auto result = shard.point_lookup(r);
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


START_TEST(t_range_query)
{
    auto buffer = create_sequential_mbuffer<Rec>(100, 1000);
    auto shard = Shard(buffer);

    rq::Parms<Rec> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rq::Query<Shard, Rec>::get_query_state(&shard, &parms);
    auto result = rq::Query<Shard, Rec>::query(&shard, state, &parms);
    rq::Query<Shard, Rec>::delete_query_state(state);

    ck_assert_int_eq(result.size(), parms.upper_bound - parms.lower_bound + 1);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_buffer_range_query)
{
    auto buffer = create_sequential_mbuffer<Rec>(100, 1000);

    rq::Parms<Rec> parms;
    parms.lower_bound = 300;
    parms.upper_bound = 500;

    auto state = rq::Query<Shard, Rec>::get_buffer_query_state(buffer, &parms);
    auto result = rq::Query<Shard, Rec>::buffer_query(buffer, state, &parms);
    rq::Query<Shard, Rec>::delete_buffer_query_state(state);

    ck_assert_int_eq(result.size(), parms.upper_bound - parms.lower_bound + 1);
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_le(result[i].rec.key, parms.upper_bound);
        ck_assert_int_ge(result[i].rec.key, parms.lower_bound);
    }

    delete buffer;
}
END_TEST


START_TEST(t_range_query_merge)
{    
    auto buffer1 = create_sequential_mbuffer<Rec>(100, 200);
    auto buffer2 = create_sequential_mbuffer<Rec>(400, 1000);

    auto shard1 = Shard(buffer1);
    auto shard2 = Shard(buffer2);

    rq::Parms<Rec> parms;
    parms.lower_bound = 150;
    parms.upper_bound = 500;

    size_t result_size = parms.upper_bound - parms.lower_bound + 1 - 200;

    auto state1 = rq::Query<Shard, Rec>::get_query_state(&shard1, &parms);
    auto state2 = rq::Query<Shard, Rec>::get_query_state(&shard2, &parms);

    std::vector<std::vector<de::Wrapped<Rec>>> results(2);
    results[0] = rq::Query<Shard, Rec>::query(&shard1, state1, &parms);
    results[1] = rq::Query<Shard, Rec>::query(&shard2, state2, &parms);

    rq::Query<Shard, Rec>::delete_query_state(state1);
    rq::Query<Shard, Rec>::delete_query_state(state2);

    ck_assert_int_eq(results[0].size() + results[1].size(), result_size);

    std::vector<std::vector<Wrapped<Rec>>> proc_results;

    for (size_t j=0; j<results.size(); j++) {
        proc_results.emplace_back(std::vector<Wrapped<Rec>>());
        for (size_t i=0; i<results[j].size(); i++) {
            proc_results[j].emplace_back(results[j][i]);
        }
    }

    auto result = rq::Query<Shard, Rec>::merge(proc_results, nullptr);
    std::sort(result.begin(), result.end());

    ck_assert_int_eq(result.size(), result_size);
    auto key = parms.lower_bound;
    for (size_t i=0; i<result.size(); i++) {
        ck_assert_int_eq(key++, result[i].key);
        if (key == 200) {
            key = 400;
        }
    }

    delete buffer1;
    delete buffer2;
}
END_TEST

START_TEST(t_lower_bound)
{
    auto buffer1 = create_sequential_mbuffer<Rec>(100, 200);
    auto buffer2 = create_sequential_mbuffer<Rec>(400, 1000);

    de::PGM<Rec> *shards[2];

    auto shard1 = Shard(buffer1);
    auto shard2 = Shard(buffer2);

    shards[0] = &shard1;
    shards[1] = &shard2;

    auto merged = Shard(shards, 2);

    for (size_t i=100; i<1000; i++) {
        Rec r;
        r.key = i;
        r.value = i;

        auto idx = merged.get_lower_bound(i);

        assert(idx < merged.get_record_count());

        auto res = merged.get_record_at(idx);

        if (i >=200 && i <400) {
            ck_assert_int_lt(res->rec.key, i);
        } else {
            ck_assert_int_eq(res->rec.key, i);
        }
    }

    delete buffer1;
    delete buffer2;
}
END_TEST


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


Suite *unit_testing()
{
    Suite *unit = suite_create("PGM Shard Unit Testing");

    TCase *create = tcase_create("de::PGM constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_irs_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);


    TCase *tombstone = tcase_create("de:PGM::tombstone cancellation Testing");
    tcase_add_test(tombstone, t_full_cancelation);
    suite_add_tcase(unit, tombstone);


    TCase *lookup = tcase_create("de:PGM:point_lookup Testing");
    tcase_add_test(lookup, t_point_lookup);
    tcase_add_test(lookup, t_point_lookup_miss);
    tcase_add_test(lookup, t_lower_bound);
    suite_add_tcase(unit, lookup);

    TCase *range_query = tcase_create("de:PGM::range_query Testing");
    tcase_add_test(range_query, t_range_query);
    tcase_add_test(range_query, t_buffer_range_query);
    tcase_add_test(range_query, t_range_query_merge);
    suite_add_tcase(unit, range_query);

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
