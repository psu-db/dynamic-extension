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
#include "framework/QueryInterface.h"
#include "framework/RecordInterface.h"
#include "framework/ExtensionStructure.h"

#include "framework/Configuration.h"
#include "framework/Scheduler.h"

#include "psu-util/timer.h"
#include "psu-ds/Alias.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L=LayoutPolicy::TEIRING, DeletePolicy D=DeletePolicy::TAGGING>
class DynamicExtension {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
public:
    DynamicExtension(size_t buffer_cap, size_t scale_factor, double max_delete_prop, size_t memory_budget=0, 
                     size_t thread_cnt=16)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(max_delete_prop)
        , m_sched(memory_budget, thread_cnt)
    {
        m_buffers.push_back(new Buffer(buffer_cap, max_delete_prop*buffer_cap));
        m_versions.push_back(new Structure(buffer_cap, scale_factor, max_delete_prop));
    }

    ~DynamicExtension() {
        for (size_t i=0; i<m_buffers.size(); i++) {
            delete m_buffers[i];
        }

        for (size_t i=0; i<m_versions.size(); i++) {
            delete m_versions[i];
        }
    }

    int insert(const R &rec) {
        return internal_append(rec, false);
    }

    int erase(const R &rec) {
        Buffer *buffer = get_buffer();

        if constexpr (D == DeletePolicy::TAGGING) {
            if (get_active_version()->tagged_delete(rec)) {
                return 1;
            }

            /*
             * the buffer will take the longest amount of time, and 
             * probably has the lowest probability of having the record,
             * so we'll check it last.
             */
            return buffer->delete_record(rec);
        }

        /*
         * If tagging isn't used, then delete using a tombstone
         */
        return internal_append(rec, true);
    }

    std::vector<R> query(void *parms) {
        auto buffer = get_buffer();
        auto vers = get_active_version();

        // Get the buffer query state
        auto buffer_state = Q::get_buffer_query_state(buffer, parms);

        // Get the shard query states
        std::vector<std::pair<ShardID, Shard*>> shards;
        std::vector<void*> states;

        for (auto &level : vers->get_levels()) {
            level->get_query_states(shards, states, parms);
        }

        Q::process_query_states(parms, states, buffer_state);

        std::vector<std::vector<Wrapped<R>>> query_results(shards.size() + 1);

        // Execute the query for the buffer
        auto buffer_results = Q::buffer_query(buffer, buffer_state, parms);
        query_results[0] = std::move(filter_deletes(buffer_results, {-1, -1}, buffer, vers));
        if constexpr (Q::EARLY_ABORT) {
            if (query_results[0].size() > 0) {
                auto result = Q::merge(query_results, parms);
                for (size_t i=0; i<states.size(); i++) {
                    Q::delete_query_state(states[i]);
                }

                Q::delete_buffer_query_state(buffer_state);
                return result;
            }
        }

        // Execute the query for each shard
        for (size_t i=0; i<shards.size(); i++) {
            auto shard_results = Q::query(shards[i].second, states[i], parms);
            query_results[i+1] = std::move(filter_deletes(shard_results, shards[i].first, buffer, vers));
            if constexpr (Q::EARLY_ABORT) {
                if (query_results[i].size() > 0) {
                    auto result = Q::merge(query_results, parms);
                    for (size_t i=0; i<states.size(); i++) {
                        Q::delete_query_state(states[i]);
                    }

                    Q::delete_buffer_query_state(buffer_state);

                    return result;
                }
            }
        }
        
        // Merge the results together
        auto result = Q::merge(query_results, parms);

        for (size_t i=0; i<states.size(); i++) {
            Q::delete_query_state(states[i]);
        }

        Q::delete_buffer_query_state(buffer_state);

        return result;
    }

    size_t get_record_count() {
        size_t cnt = get_buffer()->get_record_count();
        return cnt + get_active_version()->get_record_count();
    }

    size_t get_tombstone_cnt() {
        size_t cnt = get_buffer()->get_tombstone_count();
        return cnt + get_active_version()->get_tombstone_cnt();
    }

    size_t get_height() {
        return get_active_version()->get_height();
    }

    size_t get_memory_usage() {
        auto vers = get_active_version();
        auto buffer = get_buffer();

        return vers.get_memory_usage() + buffer->get_memory_usage();
    }

    size_t get_aux_memory_usage() {
        auto vers = get_active_version();
        auto buffer = get_buffer();

        return vers.get_aux_memory_usage() + buffer->get_aux_memory_usage();
    }

    size_t get_buffer_capacity() {
        return get_height()->get_capacity();
    }
    
    Shard *create_static_structure() {
        auto vers = get_active_version();
        std::vector<Shard *> shards;

        if (vers->get_levels().size() > 0) {
            for (int i=vers->get_levels().size() - 1; i>= 0; i--) {
                if (vers->get_levels()[i]) {
                    shards.emplace_back(vers->get_levels()[i]->get_merged_shard());
                }
            }
        }

        shards.emplace_back(new S(get_buffer()));

        Shard *shards_array[shards.size()];

        size_t j = 0;
        for (size_t i=0; i<shards.size(); i++) {
            if (shards[i]) {
                shards_array[j++] = shards[i];
            }
        }

        Shard *flattened = new S(shards_array, j);

        for (auto shard : shards) {
            delete shard;
        }

        return flattened;
    }

    /*
     * Mostly exposed for unit-testing purposes. Verifies that the current
     * active version of the ExtensionStructure doesn't violate the maximum
     * tombstone proportion invariant. 
     */
    bool validate_tombstone_proportion() {
        return get_active_version()->validate_tombstone_proportion();
    }

private:
    Scheduler<R, S, Q, L> m_sched;

    std::vector<Buffer *> m_buffers;
    std::vector<Structure *> m_versions;

    std::atomic<size_t> m_current_epoch;

    size_t m_scale_factor;
    double m_max_delete_prop;

    Buffer *get_buffer() {
        return m_buffers[0];
    }

    Structure *get_active_version() {
        return m_versions[0];
    }

    int internal_append(const R &rec, bool ts) {
        Buffer *buffer;
        while (!(buffer = get_buffer()))
            ;
        
        if (buffer->is_full()) {
            auto vers = get_active_version();
            m_sched.schedule_merge(vers, buffer);
        }

        return buffer->append(rec, ts);
    }

    std::vector<Wrapped<R>> filter_deletes(std::vector<Wrapped<R>> &records, ShardID shid, Buffer *buffer, Structure *vers) {
        if constexpr (!Q::SKIP_DELETE_FILTER) {
            return records;
        }

        std::vector<Wrapped<R>> processed_records;
        processed_records.reserve(records.size());

        // For delete tagging, we just need to check the delete bit on each
        // record.
        if constexpr (D == DeletePolicy::TAGGING) {
            for (auto &rec : records) {
                if (rec.is_deleted()) {
                    continue;
                }

                processed_records.emplace_back(rec);
            }

            return processed_records;
        }

        // For tombstone deletes, we need to search for the corresponding 
        // tombstone for each record.
        for (auto &rec : records) {
           if (rec.is_tombstone()) {
                continue;
            } 

            if (buffer->check_tombstone(rec.rec)) {
                continue;
            }

            if (shid != INVALID_SHID) {
                for (size_t lvl=0; lvl<=shid.level_idx; lvl++) {
                    if (vers->get_levels()[lvl]->check_tombstone(0, rec.rec)) {
                        continue;
                    }
                }

                if (vers->get_levels()[shid.level_idx]->check_tombstone(shid.shard_idx + 1, rec.rec)) {
                    continue;
                }
            }

            processed_records.emplace_back(rec);
        }

        return processed_records;
    }
};

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L=LayoutPolicy::TEIRING, DeletePolicy D=DeletePolicy::TAGGING>
static void de_merge_callback(DynamicExtension<R, S, Q, L, D> extension, ExtensionStructure<R, S, Q> new_version) {

}
}

