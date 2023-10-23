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
#include <set>

#include "framework/structure/MutableBuffer.h"
#include "framework/structure/InternalLevel.h"
#include "framework/interface/Shard.h"
#include "framework/interface/Query.h"
#include "framework/interface/Record.h"
#include "framework/interface/Query.h"
#include "framework/interface/Scheduler.h"
#include "framework/structure/ExtensionStructure.h"

#include "framework/util/Configuration.h"
#include "framework/scheduling/FIFOScheduler.h"
#include "framework/scheduling/Epoch.h"

#include "psu-util/timer.h"
#include "psu-ds/Alias.h"

namespace de {

template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L=LayoutPolicy::TEIRING, 
          DeletePolicy D=DeletePolicy::TAGGING, SchedulerInterface SCHED=FIFOScheduler>
class DynamicExtension {
    typedef S Shard;
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef Epoch<R, S, Q, L> Epoch;
    typedef BufferView<R, Q> BufView;

public:
    DynamicExtension(size_t buffer_cap, size_t scale_factor, double max_delete_prop, size_t memory_budget=0, 
                     size_t thread_cnt=16)
        : m_scale_factor(scale_factor)
        , m_max_delete_prop(max_delete_prop)
        , m_sched(memory_budget, thread_cnt)
        , m_buffer_capacity(buffer_cap)
        , m_buffer_delete_capacity(max_delete_prop*buffer_cap)
    {
        auto buf = new Buffer(m_buffer_capacity, m_buffer_delete_capacity);
        auto vers = new Structure(m_buffer_capacity, m_scale_factor, m_max_delete_prop);
        auto epoch = new Epoch(vers, buf);

        m_buffers.push_back(new Buffer(buffer_cap, max_delete_prop*buffer_cap));
        m_versions.push_back(new Structure(buffer_cap, scale_factor, max_delete_prop));
        m_epochs.push_back({0, epoch});
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
        if constexpr (D == DeletePolicy::TAGGING) {
            BufView buffers = get_active_epoch()->get_buffer_view();

            if (get_active_epoch()->get_structure()->tagged_delete(rec)) {
                return 1;
            }

            /*
             * the buffer will take the longest amount of time, and 
             * probably has the lowest probability of having the record,
             * so we'll check it last.
             */
            return buffers->delete_record(rec);
        }

        /*
         * If tagging isn't used, then delete using a tombstone
         */
        return internal_append(rec, true);
    }

    std::future<std::vector<R>> query(void *parms) {
        return schedule_query(get_active_epoch()->get_structure(), get_active_epoch()->get_buffers()[0], parms);
    }

    size_t get_record_count() {
        size_t cnt = get_active_epoch()->get_buffer_view().get_record_count();
        return cnt + get_active_epoch()->get_structure()->get_record_count();
    }

    size_t get_tombstone_cnt() {
        size_t cnt = get_active_epoch()->get_buffer_view().get_tombstone_count();
        return cnt + get_active_epoch()->get_structure()->get_tombstone_cnt();
    }

    size_t get_height() {
        return get_active_epoch()->get_structure()->get_height();
    }

    size_t get_memory_usage() {
        auto vers = get_active_epoch()->get_structure()->get_memory_usage();
        auto buffer = get_active_epoch()->get_buffer_view().get_memory_usage();

        return vers + buffer;
    }

    size_t get_aux_memory_usage() {
        auto vers = get_active_epoch()->get_structure()->get_aux_memory_usage();
        auto buffer = get_active_epoch()->get_buffer_view().get_aux_memory_usage();

        return vers + buffer;
    }

    size_t get_buffer_capacity() {
        return m_buffer_capacity;
    }
    
    Shard *create_static_structure() {
        auto vers = get_active_epoch()->get_structure();
        std::vector<Shard *> shards;

        if (vers->get_levels().size() > 0) {
            for (int i=vers->get_levels().size() - 1; i>= 0; i--) {
                if (vers->get_levels()[i]) {
                    shards.emplace_back(vers->get_levels()[i]->get_merged_shard());
                }
            }
        }

        // FIXME: should use a buffer view--or perhaps some sort of a 
        // raw record iterator model.
        shards.emplace_back(new S(get_active_epoch()->get_buffers()[0]));

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
        return get_active_epoch()->get_structure()->validate_tombstone_proportion();
    }

private:
    SCHED m_sched;

    std::mutex m_struct_lock;
    std::set<Buffer*> m_buffers;
    std::set<Structure *> m_versions;

    std::atomic<size_t> m_current_epoch;
    std::unordered_map<size_t, Epoch *> m_epochs;

    size_t m_scale_factor;
    double m_max_delete_prop;
    size_t m_buffer_capacity;
    size_t m_buffer_delete_capacity;

    Epoch *get_active_epoch() {
        return m_epochs[m_current_epoch.load()];
    }

    void advance_epoch() {
        size_t new_epoch_num = m_current_epoch.load() + 1;
        Epoch *new_epoch = m_epochs[new_epoch_num];
        Epoch *old_epoch = m_epochs[m_current_epoch.load()];

        // Update the new Epoch to contain the buffers
        // from the old one that it doesn't currently have
        size_t old_buffer_cnt = new_epoch->clear_buffers();
        for (size_t i=old_buffer_cnt; i<old_epoch->get_buffers().size(); i++) { 
            new_epoch->add_buffer(old_epoch->get_buffers[i]);
        }
        m_current_epoch.fetch_add(1);
    }

    /*
     * Creates a new epoch by copying the currently active one. The new epoch's
     * structure will be a shallow copy of the old one's. 
     */
    Epoch *create_new_epoch() {
        auto new_epoch = get_active_epoch()->clone();
        std::unique_lock<std::mutex> m_struct_lock;
        m_versions.insert(new_epoch->get_structure());
        m_epochs.insert({m_current_epoch.load() + 1, new_epoch});
        m_struct_lock.release();

        return new_epoch;
    }

    /*
     * Add a new empty buffer to the specified epoch. This is intended to be used
     * when a merge is triggered, to allow for inserts to be sustained in the new
     * buffer while a new epoch is being created in the background. Returns a
     * pointer to the newly created buffer.
     */
    Buffer *add_empty_buffer(Epoch *epoch) {
        auto new_buffer = Buffer(m_buffer_capacity, m_buffer_delete_capacity); 

        std::unique_lock<std::mutex> m_struct_lock;
        m_buffers.insert(new_buffer);
        m_struct_lock.release();

        epoch->add_buffer(new_buffer);
        return new_buffer;
    }

    void retire_epoch(Epoch *epoch) {
        /*
         * Epochs with currently active jobs cannot
         * be retired. By the time retire_epoch is called,
         * it is assumed that a new epoch is active, meaning
         * that the epoch to be retired should no longer
         * accumulate new active jobs. Eventually, this
         * number will hit zero and the function will
         * proceed.
         *
         * FIXME: this can be replaced with a cv, which
         * is probably a superior solution in this case
         */
        while (epoch->get_active_job_num() > 0) 
            ;

        /*
         * The epoch's destructor will handle releasing
         * all the references it holds
         */ 
        delete epoch;

        /*
         * Following the epoch's destruction, any buffers
         * or structures with no remaining references can
         * be safely freed.
         */
        std::unique_lock<std::mutex> lock(m_struct_lock);
        for (auto buf : m_buffers) {
            if (buf->get_reference_count() == 0) {
                m_buffers.erase(buf);
                delete buf;
            }
        }

        for (auto vers : m_versions) {
            if (vers->get_reference_count() == 0) {
                m_versions.erase(vers);
                delete vers;
            }
        }
    }

    static void merge(void *arguments) {
        MergeArgs<R, S, Q, L> *args = (MergeArgs<R, S, Q, L> *) arguments;

        Structure *vers = args->epoch->get_structure();
        Buffer *buff = (Buffer *) args->epoch->get_buffers()[0];

        for (ssize_t i=args->merges.size() - 1; i>=0; i--) {
            vers->merge_levels(args->merges[i].second, args->merges[i].first);
        }

        vers->merge_buffer(buff);

        args->result.set_value(true);
        args->epoch->end_job();
        delete args;
    }

    static std::vector<R> finalize_query_result(std::vector<std::vector<Wrapped<R>>> &query_results, void *parms, 
                                                std::vector<void *> &shard_states, std::vector<void *> &buffer_states) {
        auto result = Q::merge(query_results, parms);

        for (size_t i=0; i<buffer_states.size(); i++) {
            Q::delete_buffer_query_state(buffer_states[i]);
        }

        for (size_t i=0; i<states.size(); i++) {
            Q::delete_query_state(shard_states[i]);
        }

        return result;
    }

    static void async_query(void *arguments) {
        QueryArgs<R, S, Q, L> *args = (QueryArgs<R, S, Q, L> *) arguments;

        auto buffers = args->epoch->get_buffer_view();
        auto vers = args->epoch->get_structure();
        void *parms = args->query_parms;

        // Get the buffer query states
        std::vector<void *> buffer_states = buffers->get_buffer_query_states(parms);

        // Get the shard query states
        std::vector<std::pair<ShardID, Shard*>> shards;
        std::vector<void *> shard_states = vers->get_query_states(shards, parms);

        Q::process_query_states(parms, shard_states, buffer_states);

        std::vector<std::vector<Wrapped<R>>> query_results(shards.size() + buffer_states.size());

        // Execute the query for the buffer
        std::vector<std::vector<Wrapped<R>>> buffer_results(buffer_states.size());
        for (size_t i=0; i<buffer_states.size(); i++) {
            auto buffer_results = Q::buffer_query(buffers->get_buffers[i], buffer_states[i], parms);
            query_results[i] = std::move(filter_deletes(buffer_results, {-1, -1}, buffers, vers));

            if constexpr (Q::EARLY_ABORT) {
                if (query_results[i] > 0) {
                    return finalize_query_result(query_results, parms, buffer_states, shard_states);
                }
            }
        }
        
        // Execute the query for each shard
        for (size_t i=0; i<shards.size(); i++) {
            auto shard_results = Q::query(shards[i].second, states[i], parms);
            query_results[i+buffer_states.size()] = std::move(filter_deletes(shard_results, shards[i].first, buffers, vers));
            if constexpr (Q::EARLY_ABORT) {
                if (query_results[i+buffer_states.size()].size() > 0) {
                    return finalize_query_result(query_results, parms, buffer_states, shard_states);
                }
            }
        }
        
        // Merge the results together and finalize the job
        auto result = finalize_query_result(query_results, parms, buffer_states, shard_states);
        args->result_set.set_value(std::move(result));

        args->epoch->end_job();
        delete args;
    }

    std::future<bool> schedule_merge() {
        auto epoch = get_active_epoch();
        epoch->start_job();

        MergeArgs<R, S, Q, L> *args = new MergeArgs<R, S, Q, L>();
        args->epoch = epoch;
        args->merges = epoch->get_structure()->get_merge_tasks(epoch->get_buffers()[0]);
        m_sched.schedule_job(merge, 0, args);

        return args->result.get_future();
    }

    std::future<std::vector<R>> schedule_query(void *query_parms) {
        auto epoch = get_active_epoch();
        epoch->start_job();
        
        QueryArgs<R, S, Q, L> *args = new QueryArgs<R, S, Q, L>();
        args->epoch = epoch;
        args->query_parms = query_parms;
        m_sched.schedule_job(async_query, 0, args);

        return args->result_set.get_future();
    }

    int internal_append(const R &rec, bool ts) {
        Buffer *buffer;
        while (!(buffer = get_active_epoch()->get_active_buffer()))
            ;
        
        if (buffer->is_full()) {
            auto vers = get_active_epoch()->get_structure();
            auto res = schedule_merge(vers, buffer);
            res.get();
        }

        return buffer->append(rec, ts);
    }

    static std::vector<Wrapped<R>> filter_deletes(std::vector<Wrapped<R>> &records, ShardID shid, BufView *buffers, Structure *vers) {
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

            if (buffers->check_tombstone(rec.rec)) {
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
}

