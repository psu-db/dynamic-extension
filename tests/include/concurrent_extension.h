/*
 * tests/include/dynamic_extension.h
 *
 * Standardized unit tests for DynamicExtension objects
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 * WARNING: This file must be included in the main unit test set
 *          after the definition of an appropriate Shard, Query, and Rec
 *          type. In particular, Rec needs to implement the key-value
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
/*#include "testing.h"
#include "framework/DynamicExtension.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "shard/ISAMTree.h"
#include "query/rangequery.h"
#include <check.h>

//using namespace de;
//typedef DynamicExtension<Rec, ISAMTree<Rec>, rq::Query<ISAMTree<Rec>, Rec>, LayoutPolicy::LEVELING, DeletePolicy::TOMBSTONE, FIFOScheduler> DE;
*/


START_TEST(t_create)
{
    auto test_de = new DE(100, 1000, 2);

    ck_assert_ptr_nonnull(test_de);
    ck_assert_int_eq(test_de->get_record_count(), 0);
    ck_assert_int_eq(test_de->get_height(), 0);

    delete test_de;
}
END_TEST


START_TEST(t_insert)
{
    auto test_de = new DE(100, 1000, 2);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<100; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(test_de->insert(r), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(test_de->get_height(), 0);
    ck_assert_int_eq(test_de->get_record_count(), 100);

    delete test_de;
}
END_TEST


START_TEST(t_debug_insert)
{
    auto test_de = new DE(100, 1000, 2);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<1000; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(test_de->insert(r), 1);
        ck_assert_int_eq(test_de->get_record_count(), i+1);
        key++;
        val++;
    }

    delete test_de;
}
END_TEST


START_TEST(t_insert_with_mem_merges)
{
    auto test_de = new DE(100, 1000, 2);

    uint64_t key = 0;
    uint32_t val = 0;

    Rec r = {key, val};
    for (size_t i=0; i<1000; i++) {
        ck_assert_int_eq(test_de->insert(r), 1);
        r.key++;
        r.value++;
    }

    ck_assert_int_eq(test_de->get_record_count(), 1000);

    test_de->await_next_epoch();

    ck_assert_int_eq(test_de->get_record_count(), 1000);

    /* 
     * verify that we can fill past the high water mark, potentially
     * stalling to allow merges to finish as needed.
     */
    size_t cnt = 0;
    do {
        if (test_de->insert(r)) {
            r.key++;
            r.value++;
            cnt++;
            ck_assert_int_eq(test_de->get_record_count(), cnt + 1000);
        } else {
            _mm_pause();
        }
    } while (cnt < 100000);

    test_de->await_next_epoch();

    ck_assert_int_eq(test_de->get_record_count(), 101000);

    delete test_de;
}
END_TEST


START_TEST(t_range_query)
{
    auto test_de = new DE(1000, 10000, 4);
    size_t n = 10000000;

    std::vector<uint64_t> keys;
    for (size_t i=0; i<n; i++) {
        keys.push_back(i);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::shuffle(keys.begin(), keys.end(), gen);

    size_t i=0;
    while ( i < keys.size()) {
        Rec r = {keys[i], (uint32_t) i};
        if (test_de->insert(r)) {
            i++;
        } else {
            _mm_pause();
        }
    }


    test_de->await_next_epoch();

    std::sort(keys.begin(), keys.end());

    auto idx = rand() % (keys.size() - 250);

    uint64_t lower_key = keys[idx];
    uint64_t upper_key = keys[idx + 250];

    rq::Parms<Rec> p;
    p.lower_bound = lower_key;
    p.upper_bound = upper_key;

    //fprintf(stderr, "query start\n");
    auto result = test_de->query(&p);
    auto r = result.get();
    //fprintf(stderr, "query stop\n");
    std::sort(r.begin(), r.end());

    ck_assert_int_eq(r.size(), 251);

    for (size_t i=0; i<r.size(); i++) {
        ck_assert_int_eq(r[i].key, keys[idx + i]);
    }

    delete test_de;
}
END_TEST


START_TEST(t_tombstone_merging_01)
{
    size_t reccnt = 100000;
    auto test_de = new DE(100, 1000, 2);

    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    std::set<std::pair<uint64_t, uint32_t>> records; 
    std::set<std::pair<uint64_t, uint32_t>> to_delete;
    std::set<std::pair<uint64_t, uint32_t>> deleted;

    while (records.size() < reccnt) {
        uint64_t key = rand();
        uint32_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    size_t cnt=0;
    for (auto rec : records) {
        Rec r = {rec.first, rec.second};
        while (!test_de->insert(r)) {
            _mm_pause();
        }

        if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<uint64_t, uint32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                Rec dr = {del_vec[i].first, del_vec[i].second};
                while (!test_de->erase(dr)) {
                    _mm_pause();
                }
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    test_de->await_next_epoch();

    ck_assert(test_de->validate_tombstone_proportion());

    gsl_rng_free(rng);
    delete test_de;
}
END_TEST

DE *create_test_tree(size_t reccnt, size_t memlevel_cnt) {
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    auto test_de = new DE(1000, 10000, 2);

    std::set<Rec> records; 
    std::set<Rec> to_delete;
    std::set<Rec> deleted;

    while (records.size() < reccnt) {
        uint64_t key = rand();
        uint32_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    for (auto rec : records) {
        ck_assert_int_eq(test_de->insert(rec), 1);

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<Rec> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                test_de->erase(del_vec[i]);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    gsl_rng_free(rng);

    return test_de;
}

START_TEST(t_static_structure)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t reccnt = 100000;
    auto test_de = new DE(100, 1000, 2);

    std::set<Rec> records; 
    std::set<Rec> to_delete;
    std::set<Rec> deleted;

    while (records.size() < reccnt) {
        uint64_t key = rand();
        uint32_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    size_t t_reccnt = 0;
    size_t k=0;
    for (auto rec : records) {
        k++;
        while (!test_de->insert(rec)) {
            _mm_pause();
        }
        t_reccnt++;

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<Rec> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                while (!test_de->erase(del_vec[i])) {
                    _mm_pause();
                }

                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }


    //fprintf(stderr, "Tombstones: %ld\tRecords: %ld\n", test_de->get_tombstone_count(), test_de->get_record_count());
    //fprintf(stderr, "Inserts: %ld\tDeletes:%ld\tNet:%ld\n", reccnt, deletes, reccnt - deletes);

    auto flat = test_de->create_static_structure(true);
    //fprintf(stderr, "Flat: Tombstones: %ld\tRecords %ld\n", flat->get_tombstone_count(), flat->get_record_count());
    //ck_assert_int_eq(flat->get_record_count(), reccnt - deletes);

    uint64_t prev_key = 0;
    for (size_t i=0; i<flat->get_record_count(); i++) {
        auto k = flat->get_record_at(i)->rec.key;
        if (flat->get_record_at(i)->is_tombstone()) {
            fprintf(stderr, "%ld %ld %ld\n", flat->get_record_at(i-1)->rec.key,
                    flat->get_record_at(i)->rec.key, 
                    flat->get_record_at(i+1)->rec.key);
        }
     //   ck_assert(!flat->get_record_at(i)->is_tombstone());
        ck_assert_int_ge(k, prev_key);
        prev_key = k;
    }

    gsl_rng_free(rng);
    delete flat;
    delete test_de;
}
END_TEST


static void inject_dynamic_extension_tests(Suite *suite) {
    TCase *create = tcase_create("de::DynamicExtension::constructor Testing");
    tcase_add_test(create, t_create);
    suite_add_tcase(suite, create);

    TCase *insert = tcase_create("de::DynamicExtension::insert Testing");
    tcase_add_test(insert, t_insert);
    tcase_add_test(insert, t_insert_with_mem_merges);
    tcase_add_test(insert, t_debug_insert);
    tcase_set_timeout(insert, 500);
    suite_add_tcase(suite, insert);

    TCase *query = tcase_create("de::DynamicExtension::range_query Testing");
    tcase_add_test(query, t_range_query);
    tcase_set_timeout(query, 500);
    suite_add_tcase(suite, query);

    
    TCase *ts = tcase_create("de::DynamicExtension::tombstone_compaction Testing");
    tcase_add_test(ts, t_tombstone_merging_01);
    tcase_set_timeout(ts, 500);
    suite_add_tcase(suite, ts);

    TCase *flat = tcase_create("de::DynamicExtension::create_static_structure Testing");
    tcase_add_test(flat, t_static_structure);
    tcase_set_timeout(flat, 500);
    suite_add_tcase(suite, flat);
}
