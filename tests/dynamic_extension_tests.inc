/*
 * tests/dynamic_extension_tests.inc
 *
 * Unit tests for Dynamic Extension Framework
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 */

START_TEST(t_create)
{
    auto test_de = new DE(100, 2, 1);

    ck_assert_ptr_nonnull(test_de);
    ck_assert_int_eq(test_de->get_record_count(), 0);
    ck_assert_int_eq(test_de->get_height(), 0);

    delete test_de;
}
END_TEST


START_TEST(t_insert)
{
    auto test_de = new DE(100, 2, 1);

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
    auto test_de = new DE(100, 2, 1);

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
    auto test_de = new DE(100, 2, 1);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<300; i++) {
        Rec r = {key, val};
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


/*
START_TEST(t_range_sample_memtable)
{
    auto test_de = new DE(100, 2, 1);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<100; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(test_de->insert(r), 1);
        key++;
        val++;
    }

    uint64_t lower_bound = 20;
    uint64_t upper_bound = 50;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    Rec sample_set[100];

    test_de->range_sample(sample_set, lower_bound, upper_bound, 100);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete test_de;
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto test_de = new DE(100, 2, 1);

    uint64_t key = 0;
    uint32_t val = 0;
    for (size_t i=0; i<300; i++) {
        Rec r = {key, val};
        ck_assert_int_eq(test_de->insert(r), 1);
        key++;
        val++;
    }

    uint64_t lower_bound = 100;
    uint64_t upper_bound = 250;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    Rec sample_set[100];
    test_de->range_sample(sample_set, lower_bound, upper_bound, 100);

    for(size_t i=0; i<100; i++) {
        ck_assert_int_le(sample_set[i].key, upper_bound);
        ck_assert_int_ge(sample_set[i].key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete test_de;
}
END_TEST
*/

START_TEST(t_range_query)
{
    auto test_de = new DE(100, 2, 1);
    size_t n = 10000;

    std::vector<uint64_t> keys;
    for (size_t i=0; i<n; i++) {
        keys.push_back(rand() % 25000);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::shuffle(keys.begin(), keys.end(), gen);

    for (size_t i=0; i<keys.size(); i++) {
        Rec r = {keys[i], (uint32_t) i};
        ck_assert_int_eq(test_de->insert(r), 1);
    }

    test_de->await_next_epoch();

    std::sort(keys.begin(), keys.end());

    auto idx = rand() % (keys.size() - 250);

    uint64_t lower_key = keys[idx];
    uint64_t upper_key = keys[idx + 250];

    rq::Parms<Rec> p;
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
    auto test_de = new DE(100, 2, .01);

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
        ck_assert_int_eq(test_de->insert(r), 1);

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<std::pair<uint64_t, uint32_t>> del_vec;
            std::sample(to_delete.begin(), to_delete.end(), std::back_inserter(del_vec), 3, std::mt19937{std::random_device{}()});

            for (size_t i=0; i<del_vec.size(); i++) {
                Rec dr = {del_vec[i].first, del_vec[i].second};
                test_de->erase(dr);
                deletes++;
                to_delete.erase(del_vec[i]);
                deleted.insert(del_vec[i]);
            }
        }

        if (gsl_rng_uniform(rng) < 0.25 && deleted.find(rec) == deleted.end()) {
            to_delete.insert(rec);
        }

        ck_assert(test_de->validate_tombstone_proportion());
    }

    test_de->await_next_epoch();

    ck_assert(test_de->validate_tombstone_proportion());

    gsl_rng_free(rng);
    delete test_de;
}
END_TEST

DE *create_test_tree(size_t reccnt, size_t memlevel_cnt) {
    auto rng = gsl_rng_alloc(gsl_rng_mt19937);

    auto test_de = new DE(1000, 2, 1);

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
    auto test_de = new DE(100, 2, 1);

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
        ck_assert_int_eq(test_de->insert(rec), 1);
        t_reccnt++;

         if (gsl_rng_uniform(rng) < 0.05 && !to_delete.empty()) {
            std::vector<Rec> del_vec;
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


Suite *unit_testing()
{
    Suite *unit = suite_create("de::DynamicExtension Unit Testing");

    TCase *create = tcase_create("de::DynamicExtension::constructor Testing");
    tcase_add_test(create, t_create);
    suite_add_tcase(unit, create);

    TCase *insert = tcase_create("de::DynamicExtension<ISAMTree>::insert Testing");
    tcase_add_test(insert, t_insert);
    tcase_add_test(insert, t_insert_with_mem_merges);
    tcase_add_test(insert, t_debug_insert);
    suite_add_tcase(unit, insert);

    TCase *query = tcase_create("de::DynamicExtension<ISAMTree>::range_query Testing");
    tcase_add_test(query, t_range_query);
    suite_add_tcase(unit, query);

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
