/*
 * tests/mutable_buffer_tests.cpp
 *
 * Unit tests for MutableBuffer and BufferView
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */

#include <thread>
#include <vector>

#include "testing.h"
#include "framework/structure/MutableBuffer.h"

#include <check.h>

using namespace de;

START_TEST(t_create)
{
    size_t lwm = 50, hwm = 100;
    size_t cap = 2 * hwm;

    auto buffer = new MutableBuffer<Rec>(lwm, hwm);

    ck_assert_ptr_nonnull(buffer);
    ck_assert_int_eq(buffer->get_capacity(), cap);
    ck_assert_int_eq(buffer->get_low_watermark(), lwm);
    ck_assert_int_eq(buffer->get_high_watermark(), hwm);

    ck_assert_int_eq(buffer->is_full(), false);
    ck_assert_int_eq(buffer->is_at_low_watermark(), false);
    ck_assert_int_eq(buffer->get_record_count(), 0);
    ck_assert_int_eq(buffer->get_tombstone_count(), 0);

    {
        auto view = buffer->get_buffer_view();
        ck_assert_int_eq(view.get_tombstone_count(), 0);
        ck_assert_int_eq(view.get_record_count(), 0);
    }

    delete buffer;
}
END_TEST


START_TEST(t_insert)
{
    auto buffer = new MutableBuffer<Rec>(50, 100);

    Rec rec = {0, 5, 1};

    /* insert records up to the low watermark */
    size_t cnt = 0;
    for (size_t i=0; i<50; i++) {
        ck_assert_int_eq(buffer->is_at_low_watermark(), false);
        ck_assert_int_eq(buffer->append(rec), 1);
        ck_assert_int_eq(buffer->check_tombstone(rec), 0);

        rec.key++;
        rec.value++;
        cnt++;

        ck_assert_int_eq(buffer->get_record_count(), cnt);
        ck_assert_int_eq(buffer->get_buffer_view().get_record_count(), cnt);
        ck_assert_int_eq(buffer->get_tail(), cnt);
    }

    ck_assert_int_eq(buffer->is_at_low_watermark(), true);

    /* insert records up to the high watermark */
    for (size_t i=0; i<50; i++) {
        ck_assert_int_eq(buffer->is_full(), 0);
        ck_assert_int_eq(buffer->append(rec), 1);
        ck_assert_int_eq(buffer->check_tombstone(rec), 0);

        rec.key++;
        rec.value++;
        cnt++;

        ck_assert_int_eq(buffer->get_record_count(), cnt);
        ck_assert_int_eq(buffer->get_buffer_view().get_record_count(), cnt);

        ck_assert_int_eq(buffer->get_tombstone_count(), 0);
        ck_assert_int_eq(buffer->is_at_low_watermark(), true);
        ck_assert_int_eq(buffer->get_tail(), cnt);
    }

    /* further inserts should fail */
    rec.key++;
    rec.value++;
    ck_assert_int_eq(buffer->is_full(), 1);
    ck_assert_int_eq(buffer->append(rec), 0);

    delete buffer;
}
END_TEST


START_TEST(t_advance_head) 
{
    auto buffer = new MutableBuffer<Rec>(50, 100);

    /* insert 75 records and get tail when LWM is exceeded */
    size_t new_head = 0;
    Rec rec = {1, 1};
    size_t cnt = 0;
    for (size_t i=0; i<75; i++) {
        ck_assert_int_eq(buffer->append(rec), 1);

        rec.key++;
        rec.value++;
        cnt++;

        if (buffer->is_at_low_watermark() && new_head == 0) {
            new_head = buffer->get_tail();
        }
    }

    ck_assert_int_eq(buffer->get_available_capacity(), 200 - cnt);

    Wrapped<Rec> *view_records = new Wrapped<Rec>[buffer->get_record_count()];
    {
        /* get a view of the pre-advanced state */
        auto view = buffer->get_buffer_view();
        ck_assert_int_eq(view.get_record_count(), cnt);
        view.copy_to_buffer((psudb::byte *) view_records);

        /* advance the head */
        ck_assert_int_eq(buffer->advance_head(new_head), 1);
        ck_assert_int_eq(buffer->get_record_count(), 25);
        ck_assert_int_eq(buffer->get_buffer_view().get_record_count(), 25);
        ck_assert_int_eq(view.get_record_count(), cnt);
        ck_assert_int_eq(buffer->get_available_capacity(), 200 - cnt);

        /* refuse to advance head again while there remain references to the old one */
        ck_assert_int_eq(buffer->advance_head(buffer->get_tail() -1), 0);
    }

    /* once the buffer view falls out of scope, the capacity of the buffer should increase */
    ck_assert_int_eq(buffer->get_available_capacity(), 175);

    /* now the head should be able to be advanced */
    ck_assert_int_eq(buffer->advance_head(buffer->get_tail()), 1);

    /* and the buffer should be empty */
    ck_assert_int_eq(buffer->get_record_count(), 0);

    delete buffer;
    delete[] view_records;
}
END_TEST

void insert_records(std::vector<Rec> *values, size_t start, size_t stop, MutableBuffer<Rec> *buffer)
{
    for (size_t i=start; i<stop; i++) {
        buffer->append((*values)[i]);
    }

}

START_TEST(t_multithreaded_insert)
{
    size_t cnt = 10000;
    auto buffer = new MutableBuffer<Rec>(cnt/2, cnt);

    std::vector<Rec> records(cnt);
    for (size_t i=0; i<cnt; i++) {
        records[i] = Rec {(uint64_t) rand(), (uint32_t) rand()};
    }

    /* perform a multithreaded insertion */
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


Suite *unit_testing()
{
    Suite *unit = suite_create("Mutable Buffer Unit Testing");
    TCase *initialize = tcase_create("de::MutableBuffer Constructor Testing");
    tcase_add_test(initialize, t_create);

    suite_add_tcase(unit, initialize);


    TCase *append = tcase_create("de::MutableBuffer::append Testing");
    tcase_add_test(append, t_insert);
    tcase_add_test(append, t_advance_head);
    tcase_add_test(append, t_multithreaded_insert);

    suite_add_tcase(unit, append);


    TCase *truncate = tcase_create("de::MutableBuffer::truncate Testing");
    tcase_add_test(truncate, t_truncate);

    suite_add_tcase(unit, truncate);


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

