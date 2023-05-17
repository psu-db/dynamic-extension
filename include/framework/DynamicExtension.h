/*
 * include/framework/DynamicExtension.h
 *
 * Copyright (C) 2023 Douglas Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
 *
 */
#pragma once

#include <atomic>
#include <numeric>
#include <cstdio>
#include <vector>

#include "framework/MutableBuffer.h"
#include "framework/InternalLevel.h"
#include "framework/ShardInterface.h"

#include "shard/WIRS.h"
#include "ds/Alias.h"
#include "util/timer.h"

namespace de {

thread_local size_t sampling_attempts = 0;
thread_local size_t sampling_rejections = 0;
thread_local size_t deletion_rejections = 0;
thread_local size_t bounds_rejections = 0;
thread_local size_t tombstone_rejections = 0;
thread_local size_t buffer_rejections = 0;

/*
 * thread_local size_t various_sampling_times go here.
 */
thread_local size_t sample_range_time = 0;
thread_local size_t alias_time = 0;
thread_local size_t alias_query_time = 0;
thread_local size_t rejection_check_time = 0;
thread_local size_t buffer_sample_time = 0;
thread_local size_t memlevel_sample_time = 0;
thread_local size_t disklevel_sample_time = 0;
thread_local size_t sampling_bailouts = 0;


/*
 * LSM Tree configuration global variables
 */

// True for buffer rejection sampling
static constexpr bool LSM_REJ_SAMPLE = false;

// True for leveling, false for tiering
static constexpr bool LSM_LEVELING = false;

static constexpr bool DELETE_TAGGING = false;

// TODO: Replace the constexpr bools above
// with template parameters based on these
// enums.
enum class LayoutPolicy {
    LEVELING,
    TEIRING
};

enum class DeletePolicy {
    TOMBSTONE,
    TAGGING
};

typedef ssize_t level_index;

template <RecordInterface R, ShardInterface S>
class DynamicExtension {
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef decltype(R::weight) W;

public:
    DynamicExtension(size_t buffer_cap, size_t buffer_delete_cap, size_t scale_factor,
                     double max_delete_prop, double max_rejection_prop, gsl_rng *rng) 
        : m_active_buffer(0), 
          m_scale_factor(scale_factor), 
          m_max_delete_prop(max_delete_prop),
          m_max_rejection_rate(max_rejection_prop),
          m_last_level_idx(-1),
          m_buffer_1(new MutableBuffer<R>(buffer_cap, LSM_REJ_SAMPLE, buffer_delete_cap, rng)), 
          m_buffer_2(new MutableBuffer<R>(buffer_cap, LSM_REJ_SAMPLE, buffer_delete_cap, rng)),
          m_buffer_1_merging(false), m_buffer_2_merging(false) {}

    ~DynamicExtension() {
        delete m_buffer_1;
        delete m_buffer_2;

        for (size_t i=0; i<m_levels.size(); i++) {
            delete m_levels[i];
        }
    }

    int delete_record(const K& key, const V& val, gsl_rng *rng) {
        assert(DELETE_TAGGING);

        auto buffer = get_buffer();
        // Check the levels first. This assumes there aren't 
        // any undeleted duplicate records.
        for (auto level : m_levels) {
            if (level && level->delete_record(key, val)) {
                return 1;
            }
        }

        // the buffer will take the longest amount of time, and 
        // probably has the lowest probability of having the record,
        // so we'll check it last.
        return buffer->delete_record(key, val);
    }

    int append(R &rec, gsl_rng *rng) {
        // NOTE: single-threaded implementation only
        MutableBuffer<R> *buffer;
        while (!(buffer = get_buffer()))
            ;
        
        if (buffer->is_full()) {
            merge_buffer(rng);
        }

        return buffer->append(rec);
    }

    void range_sample(R *sample_set, const K& lower_key, const K& upper_key, size_t sample_sz, gsl_rng *rng) {
        auto buffer = get_buffer();
        Alias *buffer_alias = nullptr;
        std::vector<R *> buffer_records;
        size_t buffer_cutoff = 0;

        W buffer_weight;
        if (LSM_REJ_SAMPLE) {
            buffer_weight = buffer->get_total_weight(); 
            buffer_cutoff = buffer->get_record_count() - 1;
        } else {
            buffer_weight = buffer->get_sample_range(lower_key, upper_key, buffer_records, &buffer_alias, &buffer_cutoff);
        }

        // Get the shard weights for each level. Index 0 is the buffer,
        // represented by nullptr.
        std::vector<std::pair<ShardID, S*>> shards;
        std::vector<void*> states;
        shards.push_back({{-1, -1}, nullptr});
        states.push_back(nullptr);

        std::vector<W> shard_weights;
        shard_weights.push_back((double) buffer_weight);

        WIRS<R>::wirs_query_parms parms = {lower_key, upper_key};

        for (auto &level : m_levels) {
            level->get_query_states(shard_weights, shards, states, &parms);
        }

        if (shard_weights.size() == 1 && shard_weights[0] == 0) {
            if (buffer_alias) delete buffer_alias;
            for (auto& x: states) S::delete_query_state(x);
            sampling_bailouts++;
            return; // no records in the sampling range
        }

        double tot_weight = std::accumulate(shard_weights.begin(), shard_weights.end(), 0);
        std::vector<double> normalized_weights(shard_weights.size());
        for (size_t i=0; i<shard_weights.size(); i++) {
            normalized_weights[i] = ((double) shard_weights[i]) / tot_weight;
        }

        // Construct alias structure
        auto alias = Alias(normalized_weights);

        std::vector<size_t> shard_samples(shard_weights.size(), 0);

        size_t rejections = sample_sz;
        size_t sample_idx = 0;

        size_t buffer_rejections = 0;
        
        do {
            for (size_t i=0; i<rejections; i++) {
                shard_samples[alias.get(rng)] += 1;
            }

            rejections = 0;

            while (shard_samples[0] > 0) {
                const R *rec;
                if (LSM_REJ_SAMPLE) {
                    rec = buffer->get_sample(lower_key, upper_key, rng);
                } else {
                    rec = buffer_records[buffer_alias->get(rng)];
                }

                if (DELETE_TAGGING) {
                    if (rec && !rec->is_deleted()) {
                        sample_set[sample_idx++] = *rec;
                    } else {
                        rejections++;
                    }
                } else {
                    if (rec && !buffer->check_tombstone(*rec)) {
                        sample_set[sample_idx++] = *rec;
                    } else {
                        rejections++;
                    }
                }

                shard_samples[0]--;

                // Assume nothing in buffer and bail out.
                // FIXME: rather than a bailout, we could switch to non-rejection 
                // sampling, but that would require rebuilding the full alias structure. 
                // Wouldn't be too hard to do, but for the moment I'll just do this.
                if (LSM_REJ_SAMPLE && buffer_rejections >= sample_sz && sample_idx == 0 && shard_weights.size() == 1) {
                    if (buffer_alias) delete buffer_alias;
                    //for (auto& x: states) delete x;
                    sampling_bailouts++;
                    return; // no records in the sampling range
                }
            }

            std::vector<R> results;
            for (size_t i=1; i<shard_samples.size(); i++) {
                results.reserve(shard_samples[i]);

                shards[i].second->get_samples(states[i], results, lower_key, upper_key, shard_samples[i], rng);

                for (size_t j=0; j<results.size(); j++) {
                    if (rejection(results[j], shards[i].first, lower_key, upper_key, buffer, buffer_cutoff)) {
                        rejections++;
                        continue;
                    }

                    sample_set[sample_idx++] = results[j]; 
                }

                shard_samples[i] = 0;
                results.clear();
            }

        } while (sample_idx < sample_sz);

        if (buffer_alias) delete buffer_alias;
        for (auto& x: states) S::delete_query_state(x);

        enforce_rejection_rate_maximum(rng);
    }

    // Checks the tree and buffer for a tombstone corresponding to
    // the provided record in any shard *above* the shid, which
    // should correspond to the shard containing the record in question
    // 
    // Passing INVALID_SHID indicates that the record exists within the buffer
    bool is_deleted(const R &record, const ShardID &shid, MutableBuffer<R> *buffer, size_t buffer_cutoff) {
        // If tagging is enabled, we just need to check if the record has the delete tag set
        if (DELETE_TAGGING) {
            return record.is_deleted();
        }

        // Otherwise, we need to look for a tombstone.

        // check for tombstone in the buffer. This will require accounting for the cutoff eventually.
        if (buffer->check_tombstone(record)) {
            return true;
        }

        // if the record is in the buffer, then we're done.
        if (shid == INVALID_SHID) {
            return false;
        }

        for (size_t lvl=0; lvl<=shid.level_idx; lvl++) {
            if (m_levels[lvl]->check_tombstone(0, record)) {
                return true;
            }
        }

        // check the level containing the shard
        return m_levels[shid.level_idx]->check_tombstone(shid.shard_idx + 1, record);
    }


    size_t get_record_cnt() {
        size_t cnt = get_buffer()->get_record_count();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_record_cnt();
        }

        return cnt;
    }


    size_t get_tombstone_cnt() {
        size_t cnt = get_buffer()->get_tombstone_count();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_tombstone_count();
        }

        return cnt;
    }

    size_t get_height() {
        return m_levels.size();
    }

    size_t get_memory_utilization() {
        size_t cnt = m_buffer_1->get_memory_utilization() + m_buffer_2->get_memory_utilization();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) cnt += m_levels[i]->get_memory_utilization();
        }

        return cnt;
    }

    size_t get_aux_memory_utilization() {
        size_t cnt = m_buffer_1->get_aux_memory_utilization() + m_buffer_2->get_aux_memory_utilization();

        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) {
                cnt += m_levels[i]->get_aux_memory_utilization();
            }
        }

        return cnt;
    }

    bool validate_tombstone_proportion() {
        long double ts_prop;
        for (size_t i=0; i<m_levels.size(); i++) {
            if (m_levels[i]) {
                ts_prop = (long double) m_levels[i]->get_tombstone_count() / (long double) calc_level_record_capacity(i);
                if (ts_prop > (long double) m_max_delete_prop) {
                    return false;
                }
            }
        }

        return true;
    }

    size_t get_buffer_capacity() {
        return m_buffer_1->get_capacity();
    }
    

    S *create_ssi() {
        std::vector<S *> shards;

        if (m_levels.size() > 0) {
            for (int i=m_levels.size() - 1; i>= 0; i--) {
                if (m_levels[i]) {
                    shards.emplace_back(m_levels[i]->get_merged_shard());
                }
            }
        }

        shards.emplace_back(new S(get_buffer(), nullptr));

        S *shards_array[shards.size()];

        size_t j = 0;
        for (size_t i=0; i<shards.size(); i++) {
            if (shards[i]) {
                shards_array[j++] = shards[i];
            }
        }

        S *flattened = new S(shards_array, j, nullptr);

        for (auto shard : shards) {
            delete shard;
        }

        return flattened;
    }

private:
    MutableBuffer<R> *m_buffer_1;
    MutableBuffer<R> *m_buffer_2;
    std::atomic<bool> m_active_buffer;
    std::atomic<bool> m_buffer_1_merging;
    std::atomic<bool> m_buffer_2_merging;

    size_t m_scale_factor;
    double m_max_delete_prop;
    double m_max_rejection_rate;

    std::vector<InternalLevel<R, S> *> m_levels;

    level_index m_last_level_idx;

    MutableBuffer<R> *get_buffer() {
        if (m_buffer_1_merging && m_buffer_2_merging) {
            return nullptr;
        }

        return (m_active_buffer) ? m_buffer_2 : m_buffer_1;
    }

    inline bool rejection(const R &record, ShardID shid, const K& lower_bound, const K& upper_bound, MutableBuffer<R> *buffer, size_t buffer_cutoff) {
        if (record.is_tombstone()) {
            tombstone_rejections++;
            return true;
        } else if (record.key < lower_bound || record.key > upper_bound) {
            bounds_rejections++;
            return true;
        } else if (is_deleted(record, shid, buffer, buffer_cutoff)) {
            deletion_rejections++;
            return true;
        }

        return false;
    }

    inline bool add_to_sample(const R &record, ShardID shid, const K& upper_key, const K& lower_key, char *io_buffer,
                              R *sample_buffer, size_t &sample_idx, MutableBuffer<R> *buffer, size_t buffer_cutoff) {
        TIMER_INIT();
        TIMER_START();
        sampling_attempts++;
        if (!record || rejection(record, shid, lower_key, upper_key, io_buffer, buffer, buffer_cutoff)) {
            sampling_rejections++;
            return false;
        }
        TIMER_STOP();
        rejection_check_time += TIMER_RESULT();

        sample_buffer[sample_idx++] = *record;
        return true;
    }

    /*
     * Add a new level to the LSM Tree and return that level's index. Will
     * automatically determine whether the level should be on memory or on disk,
     * and act appropriately.
     */
    inline level_index grow() {
        level_index new_idx;

        size_t new_shard_cnt = (LSM_LEVELING) ? 1 : m_scale_factor;
        new_idx = m_levels.size();
        if (new_idx > 0) {
            assert(m_levels[new_idx - 1]->get_shard(0)->get_tombstone_count() == 0);
        }
        m_levels.emplace_back(new InternalLevel<R, S>(new_idx, new_shard_cnt));

        m_last_level_idx++;
        return new_idx;
    }


    // Merge the memory table down into the tree, completing any required other
    // merges to make room for it.
    inline void merge_buffer(gsl_rng *rng) {
        auto buffer = get_buffer();

        if (!can_merge_with(0, buffer->get_record_count())) {
            merge_down(0, rng);
        }

        merge_buffer_into_l0(buffer, rng);
        enforce_delete_maximum(0, rng);

        buffer->truncate();
        return;
    }

    /*
     * Merge the specified level down into the tree. The level index must be
     * non-negative (i.e., this function cannot be used to merge the buffer). This
     * routine will recursively perform any necessary merges to make room for the 
     * specified level.
     */
    inline void merge_down(level_index idx, gsl_rng *rng) {
        level_index merge_base_level = find_mergable_level(idx);
        if (merge_base_level == -1) {
            merge_base_level = grow();
        }

        for (level_index i=merge_base_level; i>idx; i--) {
            merge_levels(i, i-1, rng);
            enforce_delete_maximum(i, rng);
        }

        return;
    }

    /*
     * Find the first level below the level indicated by idx that
     * is capable of sustaining a merge operation and return its
     * level index. If no such level exists, returns -1. Also
     * returns -1 if idx==0, and no such level exists, to simplify
     * the logic of the first merge.
     */
    inline level_index find_mergable_level(level_index idx, MutableBuffer<R> *buffer=nullptr) {

        if (idx == 0 && m_levels.size() == 0) return -1;

        bool level_found = false;
        bool disk_level;
        level_index merge_level_idx;

        size_t incoming_rec_cnt = get_level_record_count(idx, buffer);
        for (level_index i=idx+1; i<=m_last_level_idx; i++) {
            if (can_merge_with(i, incoming_rec_cnt)) {
                return i;
            }

            incoming_rec_cnt = get_level_record_count(i);
        }

        return -1;
    }

    /*
     * Merge the level specified by incoming level into the level specified
     * by base level. The two levels should be sequential--i.e. no levels
     * are skipped in the merge process--otherwise the tombstone ordering
     * invariant may be violated by the merge operation.
     */
    inline void merge_levels(level_index base_level, level_index incoming_level, gsl_rng *rng) {
        // merging two memory levels
        if (LSM_LEVELING) {
            auto tmp = m_levels[base_level];
            m_levels[base_level] = InternalLevel<R, S>::merge_levels(m_levels[base_level], m_levels[incoming_level], rng);
            mark_as_unused(tmp);
        } else {
            m_levels[base_level]->append_merged_shards(m_levels[incoming_level], rng);
        }

        mark_as_unused(m_levels[incoming_level]);
        m_levels[incoming_level] = new InternalLevel<R, S>(incoming_level, (LSM_LEVELING) ? 1 : m_scale_factor);
    }


    inline void merge_buffer_into_l0(MutableBuffer<R> *buffer, gsl_rng *rng) {
        assert(m_levels[0]);
        if (LSM_LEVELING) {
            // FIXME: Kludgey implementation due to interface constraints.
            auto old_level = m_levels[0];
            auto temp_level = new InternalLevel<R, S>(0, 1);
            temp_level->append_mem_table(buffer, rng);
            auto new_level = InternalLevel<R, S>::merge_levels(old_level, temp_level, rng);

            m_levels[0] = new_level;
            delete temp_level;
            mark_as_unused(old_level);
        } else {
            m_levels[0]->append_mem_table(buffer, rng);
        }
    }

    /*
     * Mark a given memory level as no-longer in use by the tree. For now this
     * will just free the level. In future, this will be more complex as the
     * level may not be able to immediately be deleted, depending upon who
     * else is using it.
     */ 
    inline void mark_as_unused(InternalLevel<R, S> *level) {
        delete level;
    }

    /*
     * Check the tombstone proportion for the specified level and
     * if the limit is exceeded, forcibly merge levels until all
     * levels below idx are below the limit.
     */
    inline void enforce_delete_maximum(level_index idx, gsl_rng *rng) {
        long double ts_prop = (long double) m_levels[idx]->get_tombstone_count() / (long double) calc_level_record_capacity(idx);

        if (ts_prop > (long double) m_max_delete_prop) {
            merge_down(idx, rng);
        }

        return;
    }

    inline void enforce_rejection_rate_maximum(gsl_rng *rng) {
        if (m_levels.size() == 0) {
            return;
        }

        for (size_t i=0; i<m_last_level_idx; i++) {
            if (m_levels[i]) {
                double ratio = m_levels[i]->get_rejection_rate();
                if (ratio > m_max_rejection_rate) {
                    merge_down(i, rng);
                }
            }
        } 
    }

    /*
     * Assume that level "0" should be larger than the buffer. The buffer
     * itself is index -1, which should return simply the buffer capacity.
     */
    inline size_t calc_level_record_capacity(level_index idx) {
        return get_buffer()->get_capacity() * pow(m_scale_factor, idx+1);
    }

    /*
     * Returns the actual number of records present on a specified level. An
     * index value of -1 indicates the memory table. Can optionally pass in
     * a pointer to the memory table to use, if desired. Otherwise, there are
     * no guarantees about which buffer will be accessed if level_index is -1.
     */
    inline size_t get_level_record_count(level_index idx, MutableBuffer<R> *buffer=nullptr) {

        assert(idx >= -1);
        if (idx == -1) {
            return (buffer) ? buffer->get_record_count() : get_buffer()->get_record_count();
        }

        return (m_levels[idx]) ? m_levels[idx]->get_record_cnt() : 0;
    }

    /*
     * Determines if the specific level can merge with another record containing
     * incoming_rec_cnt number of records. The provided level index should be 
     * non-negative (i.e., not refer to the buffer) and will be automatically
     * translated into the appropriate index into either the disk or memory level
     * vector.
     */
    inline bool can_merge_with(level_index idx, size_t incoming_rec_cnt) {
        if (idx>= m_levels.size() || !m_levels[idx]) {
            return false;
        }

        if (LSM_LEVELING) {
            return m_levels[idx]->get_record_cnt() + incoming_rec_cnt <= calc_level_record_capacity(idx);
        } else {
            return m_levels[idx]->get_shard_count() < m_scale_factor;
        }

        // unreachable
        assert(true);
    }
};

}

