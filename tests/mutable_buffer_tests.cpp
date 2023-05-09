/*
 * tests/mutable_buffer_tests.cpp
 *
 * Unit tests for MutableBuffer
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#include <string>
#include <thread>
#include <gsl/gsl_rng.h>
#include <vector>
#include <algorithm>

#include "testing.h"
#include "framework/MutableBuffer.h"

#include <check.h>

using namespace de;

START_TEST(t_create)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto buffer = new WeightedMBuffer(100, true, 50, rng);

    ck_assert_ptr_nonnull(buffer);
    ck_assert_int_eq(buffer->get_capacity(), 100);
    ck_assert_int_eq(buffer->get_record_count(), 0);
    ck_assert_int_eq(buffer->is_full(), false);
    ck_assert_ptr_nonnull(buffer->sorted_output());
    ck_assert_int_eq(buffer->get_tombstone_count(), 0);
    ck_assert_int_eq(buffer->get_tombstone_capacity(), 50);

    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_insert)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto buffer = new WeightedMBuffer(100, true, 50, rng);

    uint64_t key = 0;
    uint32_t val = 5;

    for (size_t i=0; i<99; i++) {
        ck_assert_int_eq(buffer->append(key, val, 1, false), 1);
        ck_assert_int_eq(buffer->check_tombstone(key, val), 0);

        key++;
        val++;

        ck_assert_int_eq(buffer->get_record_count(), i+1);
        ck_assert_int_eq(buffer->get_tombstone_count(), 0);
        ck_assert_int_eq(buffer->is_full(), 0);
    }

    ck_assert_int_eq(buffer->append(key, val, 1.0, false), 1);

    key++;
    val++;

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(key, val, 1.0, false), 0);

    delete buffer;
    gsl_rng_free(rng);

}
END_TEST


START_TEST(t_insert_tombstones)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto buffer = new WeightedMBuffer(100, true, 50, rng);

    uint64_t key = 0;
    uint32_t val = 5;
    size_t ts_cnt = 0;

    for (size_t i=0; i<99; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(buffer->append(key, val, 1.0, ts), 1);
        ck_assert_int_eq(buffer->check_tombstone(key, val), ts);

        key++;
        val++;

        ck_assert_int_eq(buffer->get_record_count(), i+1);
        ck_assert_int_eq(buffer->get_tombstone_count(), ts_cnt);
        ck_assert_int_eq(buffer->is_full(), 0);
    }

    // inserting one more tombstone should not be possible
    ck_assert_int_eq(buffer->append(key, val, 1.0, true), 0);


    ck_assert_int_eq(buffer->append(key, val, 1.0, false), 1);

    key++;
    val++;

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(key, val, 1.0, false), 0);

    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


START_TEST(t_truncate)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto buffer = new WeightedMBuffer(100, true, 100, rng);

    uint64_t key = 0;
    uint32_t val = 5;
    size_t ts_cnt = 0;

    for (size_t i=0; i<100; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(buffer->append(key, val, 1.0, ts), 1);
        ck_assert_int_eq(buffer->check_tombstone(key, val), ts);

        key++;
        val++;

        ck_assert_int_eq(buffer->get_record_count(), i+1);
        ck_assert_int_eq(buffer->get_tombstone_count(), ts_cnt);
    }

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(key, val, 1.0, false), 0);

    ck_assert_int_eq(buffer->truncate(), 1);

    ck_assert_int_eq(buffer->is_full(), 0);
    ck_assert_int_eq(buffer->get_record_count(), 0);
    ck_assert_int_eq(buffer->get_tombstone_count(), 0);
    ck_assert_int_eq(buffer->append(key, val, 1.0, false), 1);

    delete buffer;
    gsl_rng_free(rng);

}
END_TEST


START_TEST(t_sorted_output)
{
    size_t cnt = 100;

    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto buffer = new WeightedMBuffer(cnt, true, cnt/2, rng);


    std::vector<uint64_t> keys(cnt);
    for (size_t i=0; i<cnt-2; i++) {
        keys[i] = rand();
    }

    // duplicate final two records for tombstone testing
    // purposes
    keys[cnt-2] =  keys[cnt-3];
    keys[cnt-1] =  keys[cnt-2];

    uint32_t val = 12345;
    for (size_t i=0; i<cnt-2; i++) {
        buffer->append(keys[i], val, 1.0, false);
    }

    buffer->append(keys[cnt-2], val, 1.0, true);
    buffer->append(keys[cnt-1], val, 1.0, true);


    WeightedRec *sorted_records = buffer->sorted_output();
    std::sort(keys.begin(), keys.end());

    for (size_t i=0; i<cnt; i++) {
        ck_assert_int_eq(sorted_records[i].key, keys[i]);
    }

    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


void insert_records(std::vector<std::pair<uint64_t, uint32_t>> *values, size_t start, size_t stop, WeightedMBuffer *buffer)
{
    for (size_t i=start; i<stop; i++) {
        buffer->append((*values)[i].first, (*values)[i].second, 1.0);
    }

}

START_TEST(t_multithreaded_insert)
{
    size_t cnt = 10000;
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);
    auto buffer = new WeightedMBuffer(cnt, true, cnt/2, rng);

    std::vector<std::pair<uint64_t, uint32_t>> records(cnt);
    for (size_t i=0; i<cnt; i++) {
        records[i] = {rand(), rand()};
    }

    // perform a t_multithreaded insertion
    size_t thread_cnt = 8;
    size_t per_thread = cnt / thread_cnt;
    std::vector<std::thread> workers(thread_cnt);
    size_t start = 0;
    size_t stop = start + per_thread;
    for (size_t i=0; i<thread_cnt; i++) {
        workers[i] = std::thread(insert_records, &records, start, stop, buffer);
        start = stop;
        stop = std::min(start + per_thread, cnt);
    }

    for (size_t i=0; i<thread_cnt; i++) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->get_record_count(), cnt);

    std::sort(records.begin(), records.end());
    WeightedRec *sorted_records = buffer->sorted_output();
    for (size_t i=0; i<cnt; i++) {
        ck_assert_int_eq(sorted_records[i].key, records[i].first);
    }

    delete buffer;
    gsl_rng_free(rng);
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("Mutable Buffer Unit Testing");
    TCase *initialize = tcase_create("de::MutableBuffer Constructor Testing");
    tcase_add_test(initialize, t_create);

    suite_add_tcase(unit, initialize);


    TCase *append = tcase_create("de::MutableBuffer::append Testing");
    tcase_add_test(append, t_insert);
    tcase_add_test(append, t_insert_tombstones);
    tcase_add_test(append, t_multithreaded_insert);

    suite_add_tcase(unit, append);


    TCase *truncate = tcase_create("de::MutableBuffer::truncate Testing");
    tcase_add_test(truncate, t_truncate);

    suite_add_tcase(unit, truncate);


    TCase *sorted_out = tcase_create("de::MutableBuffer::sorted_output");
    tcase_add_test(sorted_out, t_sorted_output);

    suite_add_tcase(unit, sorted_out);

    return unit;
}


int run_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_runner = srunner_create(unit);

    srunner_run_all(unit_runner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_runner);
    srunner_free(unit_runner);

    return failed;
}


int main() 
{
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

