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
 *          after the definition of an appropriate Shard, Query, and R
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
//#include "testing.h"
//#include "framework/DynamicExtension.h"
//#include "framework/scheduling/SerialScheduler.h"
//#include "shard/ISAMTree.h"
//#include "query/rangequery.h"
//#include <check.h>
//using namespace de;
//typedef DynamicExtension<R, ISAMTree<R>, rq::Query<ISAMTree<R>, R>, LayoutPolicy::TEIRING, DeletePolicy::TAGGING, SerialScheduler> DE;


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
        R r = {key, val};
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
        R r = {key, val};
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
    for (size_t i=0; i<300; i++) {
        R r = {key, val};
        ck_assert_int_eq(test_de->insert(r), 1);
        key++;
        val++;
    }

    test_de->await_next_epoch();

    ck_assert_int_eq(test_de->get_record_count(), 300);
    ck_assert_int_eq(test_de->get_height(), 1);

    delete test_de;
}
END_TEST


START_TEST(t_range_query)
{
    auto test_de = new DE(100, 1000, 2);
    size_t n = 10000;

    std::vector<uint64_t> keys;
    for (size_t i=0; i<n; i++) {
        keys.push_back(rand() % 25000);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::shuffle(keys.begin(), keys.end(), gen);

    for (size_t i=0; i<keys.size(); i++) {
        R r = {keys[i], (uint32_t) i};
        ck_assert_int_eq(test_de->insert(r), 1);
    }

    test_de->await_next_epoch();

    std::sort(keys.begin(), keys.end());

    auto idx = rand() % (keys.size() - 250);

    uint64_t lower_key = keys[idx];
    uint64_t upper_key = keys[idx + 250];

    rq::Parms<R> p;
    p.lower_bound = lower_key;
    p.upper_bound = upper_key;

    auto result = test_de->query(&p);
    auto r = result.get();
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
        R r = {rec.first, rec.second};
        ck_assert_int_eq(test_de->insert(r), 1);

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<uint64_t, uint32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                R dr = {del_vec[i].first, del_vec[i].second};
                test_de->erase(dr);
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

    std::set<R> records; 
    std::set<R> to_delete;
    std::set<R> deleted;

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
            std::vector<R> del_vec;
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

    std::set<R> records; 
    std::set<R> to_delete;
    std::set<R> deleted;

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
        ck_assert_int_eq(test_de->insert(rec), 1);
        t_reccnt++;

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<R> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                ck_assert_int_eq(test_de->erase(del_vec[i]), 1);

                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    auto flat = test_de->create_static_structure();
    ck_assert_int_eq(flat->get_record_count(), reccnt - deletes);

    uint64_t prev_key = 0;
    for (size_t i=0; i<flat->get_record_count(); i++) {
        auto k = flat->get_record_at(i)->rec.key;
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
    suite_add_tcase(suite, insert);

    TCase *query = tcase_create("de::DynamicExtension::range_query Testing");
    tcase_add_test(query, t_range_query);
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
