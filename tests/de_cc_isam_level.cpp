/*
 * tests/de_cc_isam_level.cpp
 *
 * Unit tests for Dynamic Extension Framework
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */

#include <random>

#include "framework/DynamicExtension.h"
#include "shard/MemISAM.h"

#include <check.h>

using namespace de;

typedef Record<int32_t, int32_t> Rec;
typedef DynamicExtension<Rec, MemISAM<Rec>, ISAMRangeQuery<Rec>, LayoutPolicy::LEVELING, DeletePolicy::TOMBSTONE> DE;

START_TEST(t_create)
{
    auto de_isam = new DE(100, 2, 1);

    ck_assert_ptr_nonnull(de_isam);
    ck_assert_int_eq(de_isam->get_record_count(), 0);
    ck_assert_int_eq(de_isam->get_height(), 0);

    delete de_isam;
}
END_TEST


START_TEST(t_insert)
{
    auto ext_wirs = new DE(100, 2, 1);

    int32_t key = 0;
    int32_t val = 0;
    for (size_t i=0; i<100; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(ext_wirs->insert(r), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(ext_wirs->get_height(), 0);
    ck_assert_int_eq(ext_wirs->get_record_count(), 100);

    delete ext_wirs;
}
END_TEST


START_TEST(t_debug_insert)
{
    auto ext_wirs = new DE(100, 2, 1);

    int32_t key = 0;
    int32_t val = 0;
    for (size_t i=0; i<1000; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(ext_wirs->insert(r), 1);
        ck_assert_int_eq(ext_wirs->get_record_count(), i+1);
        key++;
        val++;
    }

    delete ext_wirs;
}
END_TEST


START_TEST(t_insert_with_mem_merges)
{
    auto ext_wirs = new DE(100, 2, 1);

    int32_t key = 0;
    int32_t val = 0;
    for (size_t i=0; i<300; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(ext_wirs->insert(r), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(ext_wirs->get_record_count(), 300);
    ck_assert_int_eq(ext_wirs->get_height(), 1);

    delete ext_wirs;
}
END_TEST


/*
START_TEST(t_range_sample_memtable)
{
    auto ext_wirs = new DE(100, 2, 1);

    int32_t key = 0;
    int32_t val = 0;
    for (size_t i=0; i<100; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(ext_wirs->insert(r), 1);
        key++;
        val++;
    }

    int32_t lower_bound = 20;
    int32_t upper_bound = 50;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    Rec sample_set[100];

    ext_wirs->range_sample(sample_set, lower_bound, upper_bound, 100);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete ext_wirs;
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto ext_wirs = new DE(100, 2, 1);

    int32_t key = 0;
    int32_t val = 0;
    for (size_t i=0; i<300; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(ext_wirs->insert(r), 1);
        key++;
        val++;
    }

    int32_t lower_bound = 100;
    int32_t upper_bound = 250;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    Rec sample_set[100];
    ext_wirs->range_sample(sample_set, lower_bound, upper_bound, 100);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete ext_wirs;
}
END_TEST

START_TEST(t_range_sample_weighted)
{
    auto ext_wirs = new DE(100, 2, 1);
    size_t n = 10000;

    std::vector<int32_t> keys;

    int32_t key = 1;
    for (size_t i=0; i< n / 2; i++) {
        keys.push_back(key);
    }

    // put in a quarter of the count with weight two.
    key = 2;
    for (size_t i=0; i< n / 4; i++) {
        keys.push_back(key);
    }

    // the remaining quarter with weight four.
    key = 3;
    for (size_t i=0; i< n / 4; i++) {
        keys.push_back(key);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::shuffle(keys.begin(), keys.end(), gen);

    for (size_t i=0; i<keys.size(); i++) {
        int32_t weight;
        if (keys[i] == 1)  {
            weight = 2;
        } else if (keys[i] == 2) {
            weight = 4;
        } else {
            weight = 8;
        }

        Rec r = {keys[i], (int32_t) i, weight};
        ext_wirs->insert(r);
    }
    size_t k = 1000;
    int32_t lower_key = 0;
    int32_t upper_key = 5;

    size_t cnt[3] = {0};
    size_t total_samples = 0;

    wirs_query_parms<Rec> p;
    p.lower_bound = lower_key;
    p.upper_bound = upper_key;
    p.sample_size = k;
    p.rng = gsl_rng_alloc(gsl_rng_mt19937);

    for (size_t i=0; i<1000; i++) {

        auto result = ext_wirs->query(&p);
        auto r = result.get();
        total_samples += r.size();

        for (size_t j=0; j<r.size(); j++) {
            cnt[r[j].key - 1]++;
        }
    }

    ck_assert(roughly_equal(cnt[0], (double) total_samples/4.0, total_samples, .03));
    ck_assert(roughly_equal(cnt[1], (double) total_samples/4.0, total_samples, .03));
    ck_assert(roughly_equal(cnt[2], (double) total_samples/2.0, total_samples, .03));

    gsl_rng_free(p.rng);
    delete ext_wirs;
}
END_TEST

*/


START_TEST(t_tombstone_merging_01)
{
    size_t reccnt = 100000;
    auto ext_wirs = new DE(100, 2, .01);

    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    std::set<std::pair<int32_t, int32_t>> records; 
    std::set<std::pair<int32_t, int32_t>> to_delete;
    std::set<std::pair<int32_t, int32_t>> deleted;

    while (records.size() < reccnt) {
        int32_t key = rand();
        int32_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    size_t cnt=0;
    for (auto rec : records) {
        Rec r = {rec.first, rec.second, 1};
        ck_assert_int_eq(ext_wirs->insert(r), 1);

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<int32_t, int32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                Rec dr = {del_vec[i].first, del_vec[i].second, 1};
                ext_wirs->erase(dr);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }

        ck_assert(ext_wirs->validate_tombstone_proportion());
    }

    ck_assert(ext_wirs->validate_tombstone_proportion());

    gsl_rng_free(rng);
    delete ext_wirs;
}
END_TEST

DE *create_test_tree(size_t reccnt, size_t memlevel_cnt) {
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    auto ext_wirs = new DE(1000, 2, 1);

    std::set<Rec> records; 
    std::set<Rec> to_delete;
    std::set<Rec> deleted;

    while (records.size() < reccnt) {
        int32_t key = rand();
        int32_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    for (auto rec : records) {
        ck_assert_int_eq(ext_wirs->insert(rec), 1);

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<Rec> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                ext_wirs->erase(del_vec[i]);
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

    return ext_wirs;
}

START_TEST(t_static_structure)
{
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    size_t reccnt = 100000;
    auto ext_wirs = new DE(100, 2, 1);

    std::set<Rec> records; 
    std::set<Rec> to_delete;
    std::set<Rec> deleted;

    while (records.size() < reccnt) {
        int32_t key = rand();
        int32_t val = rand();

        if (records.find({key, val}) != records.end()) continue;

        records.insert({key, val});
    }

    size_t deletes = 0;
    size_t t_reccnt = 0;
    size_t k=0;
    for (auto rec : records) {
        k++;
        ck_assert_int_eq(ext_wirs->insert(rec), 1);
        t_reccnt++;

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<Rec> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                ck_assert_int_eq(ext_wirs->erase(del_vec[i]), 1);

                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    auto flat = ext_wirs->create_static_structure();
    ck_assert_int_eq(flat->get_record_count(), reccnt - deletes);

    int32_t prev_key = 0;
    for (size_t i=0; i<flat->get_record_count(); i++) {
        auto k = flat->get_record_at(i)->rec.key;
        ck_assert_int_ge(k, prev_key);
        prev_key = k;
    }

    gsl_rng_free(rng);
    delete flat;
    delete ext_wirs;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("de::DynamicExtension Unit Testing");

    TCase *create = tcase_create("de::DynamicExtension::constructor Testing");
    tcase_add_test(create, t_create);
    suite_add_tcase(unit, create);

    TCase *insert = tcase_create("de::DynamicExtension<WIRS>::insert Testing");
    tcase_add_test(insert, t_insert);
    tcase_add_test(insert, t_insert_with_mem_merges);
    tcase_add_test(insert, t_debug_insert);
    suite_add_tcase(unit, insert);


    /*
    TCase *sampling = tcase_create("de::DynamicExtension<WIRS>::range_sample Testing");

    tcase_add_test(sampling, t_range_sample_weighted);
    suite_add_tcase(unit, sampling);
    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    */

    TCase *ts = tcase_create("de::DynamicExtension::tombstone_compaction Testing");
    tcase_add_test(ts, t_tombstone_merging_01);
    tcase_set_timeout(ts, 500);
    suite_add_tcase(unit, ts);

    TCase *flat = tcase_create("de::DynamicExtension::create_static_structure Testing");
    tcase_add_test(flat, t_static_structure);
    tcase_set_timeout(flat, 500);
    suite_add_tcase(unit, flat);

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
