#include <set>
#include <random>
#include <algorithm>

#include "testing.h"
#include "framework/DynamicExtension.h"

#include <check.h>
using namespace de;

typedef DynamicExtension<uint64_t, uint32_t, uint64_t> DE_WIRS;

START_TEST(t_create)
{
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);


    ck_assert_ptr_nonnull(ext_wirs);
    ck_assert_int_eq(ext_wirs->get_record_cnt(), 0);
    ck_assert_int_eq(ext_wirs->get_height(), 0);

    delete ext_wirs;
}
END_TEST


START_TEST(t_append)
{
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<100; i++) {
        ck_assert_int_eq(ext_wirs->append(key, val, 1, false, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(ext_wirs->get_height(), 0);
    ck_assert_int_eq(ext_wirs->get_record_cnt(), 100);

    delete ext_wirs;
}
END_TEST


START_TEST(t_append_with_mem_merges)
{
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<300; i++) {
        ck_assert_int_eq(ext_wirs->append(key, val, 1, false, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(ext_wirs->get_record_cnt(), 300);
    ck_assert_int_eq(ext_wirs->get_height(), 1);

    delete ext_wirs;
}
END_TEST


START_TEST(t_range_sample_memtable)
{
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<100; i++) {
        ck_assert_int_eq(ext_wirs->append(key, val, 1, false, g_rng), 1);
        key++;
        val++;
    }

    uint64_t lower_bound = 20;
    uint64_t upper_bound = 50;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    WeightedRec sample_set[100];

    ext_wirs->range_sample(sample_set, lower_bound, upper_bound, 100, g_rng);

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
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<300; i++) {
        ck_assert_int_eq(ext_wirs->append(key, val, 1, false, g_rng), 1);
        key++;
        val++;
    }

    uint64_t lower_bound = 100;
    uint64_t upper_bound = 250;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    WeightedRec sample_set[100];
    ext_wirs->range_sample(sample_set, lower_bound, upper_bound, 100, g_rng);

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
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);
    size_t n = 10000;

    std::vector<uint64_t> keys;

    uint64_t key = 1;
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
        double weight;
        if (keys[i] == 1)  {
            weight = 2.0;
        } else if (keys[i] == 2) {
            weight = 4.0;
        } else {
            weight = 8.0;
        }

        ext_wirs->append(keys[i], i, weight, false, g_rng);
    }
    size_t k = 1000;
    uint64_t lower_key = 0;
    uint64_t upper_key = 5;

    WeightedRec* buffer = new WeightedRec[k]();
    char *buffer1 = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *buffer2 = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    size_t cnt[3] = {0};
    for (size_t i=0; i<1000; i++) {
        ext_wirs->range_sample(buffer, lower_key, upper_key, k, g_rng);

        for (size_t j=0; j<k; j++) {
            cnt[buffer[j].key - 1]++;
        }
    }

    ck_assert(roughly_equal(cnt[0] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[1] / 1000, (double) k/4.0, k, .05));
    ck_assert(roughly_equal(cnt[2] / 1000, (double) k/2.0, k, .05));

    delete ext_wirs;
    delete[] buffer;
    free(buffer1);
    free(buffer2);
}
END_TEST


START_TEST(t_tombstone_merging_01)
{
    size_t reccnt = 100000;
    auto ext_wirs = new DE_WIRS(100, 100, 2, .01, 1, g_rng);

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
        ck_assert_int_eq(ext_wirs->append(rec.first, rec.second, 1, false, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<uint64_t, uint32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                ext_wirs->append(del_vec[i].first, del_vec[i].second, 1, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }

        ck_assert(ext_wirs->validate_tombstone_proportion());
    }

    ck_assert(ext_wirs->validate_tombstone_proportion());

    delete ext_wirs;
}
END_TEST

DE_WIRS *create_test_tree(size_t reccnt, size_t memlevel_cnt) {
    auto ext_wirs = new DE_WIRS(1000, 1000, 2, 1, 1, g_rng);

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
    for (auto rec : records) {
        ck_assert_int_eq(ext_wirs->append(rec.first, rec.second, 1, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<uint64_t, uint32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                ext_wirs->append(del_vec[i].first, del_vec[i].second, 1, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    return ext_wirs;
}

START_TEST(t_sorted_array)
{
    size_t reccnt = 100000;
    auto ext_wirs = new DE_WIRS(100, 100, 2, 1, 1, g_rng);

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
    for (auto rec : records) {
        ck_assert_int_eq(ext_wirs->append(rec.first, rec.second, 1, 0, g_rng), 1);

         if (gsl_rng_uniform(g_rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<uint64_t, uint32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                ext_wirs->append(del_vec[i].first, del_vec[i].second, 1, true, g_rng);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(g_rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }
    }

    auto flat = ext_wirs->create_ssi();
    ck_assert_int_eq(flat->get_record_count(), reccnt - deletes);

    uint64_t prev_key = 0;
    for (size_t i=0; i<flat->get_record_count(); i++) {
        auto k = flat->get_record_at(i)->key;
        ck_assert_int_ge(k, prev_key);
        prev_key = k;
    }

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

    TCase *append = tcase_create("de::DynamicExtension::append Testing");
    tcase_add_test(append, t_append);
    tcase_add_test(append, t_append_with_mem_merges);
    suite_add_tcase(unit, append);

    TCase *sampling = tcase_create("de::DynamicExtension::range_sample Testing");

    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_weighted);
    suite_add_tcase(unit, sampling);

    TCase *ts = tcase_create("de::DynamicExtension::tombstone_compaction Testing");
    tcase_add_test(ts, t_tombstone_merging_01);
    tcase_set_timeout(ts, 500);
    suite_add_tcase(unit, ts);

    TCase *flat = tcase_create("de::DynamicExtension::get_flattened_wirs_run Testing");
    tcase_add_test(flat, t_sorted_array);
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
