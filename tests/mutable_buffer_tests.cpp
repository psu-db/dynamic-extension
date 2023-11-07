/*
 * tests/mutable_buffer_tests.cpp
 *
 * Unit tests for MutableBuffer
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */
#include <string>
#include <thread>
#include <vector>
#include <algorithm>

#include "testing.h"
#include "framework/structure/MutableBuffer.h"

#include <check.h>

#define DE_MT_TEST 0

using namespace de;

START_TEST(t_create)
{
    auto buffer = new MutableBuffer<Rec>(100, 50);

    ck_assert_ptr_nonnull(buffer);
    ck_assert_int_eq(buffer->get_capacity(), 100);
    ck_assert_int_eq(buffer->get_record_count(), 0);
    ck_assert_int_eq(buffer->is_full(), false);
    ck_assert_ptr_nonnull(buffer->get_data());
    ck_assert_int_eq(buffer->get_tombstone_count(), 0);
    ck_assert_int_eq(buffer->get_tombstone_capacity(), 50);

    delete buffer;
}
END_TEST


START_TEST(t_insert)
{
    auto buffer = new MutableBuffer<WRec>(100, 50);

    uint64_t key = 0;
    uint32_t val = 5;

    WRec rec = {0, 5, 1};

    for (size_t i=0; i<99; i++) {
        ck_assert_int_eq(buffer->append(rec), 1);
        ck_assert_int_eq(buffer->check_tombstone(rec), 0);

        rec.key++;
        rec.value++;

        ck_assert_int_eq(buffer->get_record_count(), i+1);
        ck_assert_int_eq(buffer->get_tombstone_count(), 0);
        ck_assert_int_eq(buffer->is_full(), 0);
    }

    ck_assert_int_eq(buffer->append(rec), 1);

    rec.key++;
    rec.value++;

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(rec), 0);

    delete buffer;

}
END_TEST


START_TEST(t_insert_tombstones)
{
    auto buffer = new MutableBuffer<Rec>(100, 50);

    size_t ts_cnt = 0;

    Rec rec = {0, 5};

    for (size_t i=0; i<99; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(buffer->append(rec, ts), 1);
        ck_assert_int_eq(buffer->check_tombstone(rec), ts);

        rec.key++;
        rec.value++;

        ck_assert_int_eq(buffer->get_record_count(), i+1);
        ck_assert_int_eq(buffer->get_tombstone_count(), ts_cnt);
        ck_assert_int_eq(buffer->is_full(), 0);
    }

    // inserting one more tombstone should not be possible
    ck_assert_int_eq(buffer->append(rec, true), 0);


    ck_assert_int_eq(buffer->append(rec), 1);

    rec.key++;
    rec.value++;

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(rec), 0);

    delete buffer;
}
END_TEST


START_TEST(t_truncate)
{
    auto buffer = new MutableBuffer<Rec>(100, 100);

    size_t ts_cnt = 0;
    Rec rec = {0, 5};

    for (size_t i=0; i<100; i++) {
        bool ts = false;
        if (i % 2 == 0) {
            ts_cnt++;
            ts=true;
        }

        ck_assert_int_eq(buffer->append(rec, ts), 1);
        ck_assert_int_eq(buffer->check_tombstone(rec), ts);

        rec.key++;
        rec.value++;

        ck_assert_int_eq(buffer->get_record_count(), i+1);
        ck_assert_int_eq(buffer->get_tombstone_count(), ts_cnt);
    }

    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(rec), 0);

    ck_assert_int_eq(buffer->truncate(), 1);

    ck_assert_int_eq(buffer->is_full(), 0);
    ck_assert_int_eq(buffer->get_record_count(), 0);
    ck_assert_int_eq(buffer->get_tombstone_count(), 0);
    ck_assert_int_eq(buffer->append(rec), 1);

    delete buffer;

}
END_TEST


START_TEST(t_get_data)
{
    size_t cnt = 100;

    auto buffer = new MutableBuffer<Rec>(cnt, cnt/2);


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
        buffer->append(Rec {keys[i], val});
    }

    Rec r1 = {keys[cnt-2], val};
    buffer->append(r1, true);

    Rec r2 = {keys[cnt-1], val};
    buffer->append(r2, true);


    auto *sorted_records = buffer->get_data();
    std::sort(keys.begin(), keys.end());
    std::sort(sorted_records, sorted_records + buffer->get_record_count(), std::less<Wrapped<Rec>>());

    for (size_t i=0; i<cnt; i++) {
        ck_assert_int_eq(sorted_records[i].rec.key, keys[i]);
    }

    delete buffer;
}
END_TEST


void insert_records(std::vector<std::pair<uint64_t, uint32_t>> *values, size_t start, size_t stop, MutableBuffer<Rec> *buffer)
{
    for (size_t i=start; i<stop; i++) {
        buffer->append({(*values)[i].first, (*values)[i].second});
    }

}

#if DE_MT_TEST
START_TEST(t_multithreaded_insert)
{
    size_t cnt = 10000;
    auto buffer = new MutableBuffer<Rec>(cnt, true, cnt/2);

    std::vector<Rec> records(cnt);
    for (size_t i=0; i<cnt; i++) {
        records[i] = Rec {(uint64_t) rand(), (uint32_t) rand()};
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
    auto *sorted_records = buffer->sorted_output();
    for (size_t i=0; i<cnt; i++) {
        ck_assert_int_eq(sorted_records[i].key, records[i].key);
    }

    delete buffer;
}
END_TEST
#endif


Suite *unit_testing()
{
    Suite *unit = suite_create("Mutable Buffer Unit Testing");
    TCase *initialize = tcase_create("de::MutableBuffer Constructor Testing");
    tcase_add_test(initialize, t_create);

    suite_add_tcase(unit, initialize);


    TCase *append = tcase_create("de::MutableBuffer::append Testing");
    tcase_add_test(append, t_insert);
    tcase_add_test(append, t_insert_tombstones);
    #if DE_MT_TEST
        tcase_add_test(append, t_multithreaded_insert);
    #endif

    suite_add_tcase(unit, append);


    TCase *truncate = tcase_create("de::MutableBuffer::truncate Testing");
    tcase_add_test(truncate, t_truncate);

    suite_add_tcase(unit, truncate);


    TCase *sorted_out = tcase_create("de::MutableBuffer::get_data");
    tcase_add_test(sorted_out, t_get_data);

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

