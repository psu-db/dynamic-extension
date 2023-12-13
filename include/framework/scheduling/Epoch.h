/*
 * include/framework/scheduling/Epoch.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *
 * Distributed under the Modified BSD License.
 *
 */
#pragma once

#include "framework/structure/MutableBuffer.h"
#include "framework/structure/ExtensionStructure.h"
#include "framework/structure/BufferView.h"

namespace de {


template <RecordInterface R, ShardInterface S, QueryInterface Q, LayoutPolicy L>
class Epoch {
private:
    typedef MutableBuffer<R> Buffer;
    typedef ExtensionStructure<R, S, Q, L> Structure;
    typedef BufferView<R, Q> BufView;
public:
    Epoch(size_t number=0)
        : m_buffers()
        , m_structure(nullptr)
        , m_active_merge(false)
        , m_active_jobs(0) 
        , m_active(true)
        , m_epoch_number(number)
    {}

    Epoch(size_t number, Structure *structure, Buffer *buff) 
        : m_buffers()
        , m_structure(structure)
        , m_active_jobs(0) 
        , m_active_merge(false)
        , m_active(true)
        , m_epoch_number(number)
    {
        structure->take_reference();
        buff->take_reference();
        m_buffers.push_back(buff);
    }

    ~Epoch() {
        assert(m_active_jobs.load() == 0);

        /* FIXME: this is needed to keep the destructor from
         * sometimes locking up here. But there *shouldn't* be
         * any threads waiting on this signal at object destruction,
         * so something else is going on here that needs looked into
         */
        //m_active_cv.notify_all();

        clear_buffers();

        if (m_structure) {
            m_structure->release_reference();
        }
    }

    Buffer *add_buffer(Buffer *buf, Buffer *cur_buf=nullptr) {
        assert(buf);

        std::unique_lock<std::mutex> m_buffer_lock;
        /* 
         * if a current buffer is specified, only add the
         * new buffer if the active buffer is the current,
         * otherwise just return the active buffer (poor man's
         * CAS).
         */
        if (cur_buf) {
            auto active_buf = get_active_buffer();
            if (active_buf != cur_buf) {
                return active_buf;
            }
        }

        buf->take_reference();
        m_buffers.push_back(buf);
        return buf;
    }

    void start_job() {
        m_active_jobs.fetch_add(1);
    }

    void end_job() {
        assert(m_active_jobs.load() > 0);
        m_active_jobs.fetch_add(-1);

        if (m_active_jobs.load() == 0) {
            std::unique_lock<std::mutex> lk(m_cv_lock);
            m_active_cv.notify_all();  
        }
    }

    size_t get_active_job_num() {
        return m_active_jobs.load();
    }

    size_t get_epoch_number() {
        return m_epoch_number;
    }

    Structure *get_structure() {
        return m_structure;
    }

    std::vector<Buffer *> &get_buffers() {
        return m_buffers;
    }

    BufView get_buffer_view() {
        std::unique_lock<std::mutex> m_buffer_lock;
        return BufView(m_buffers);
    }

    Buffer *get_active_buffer() {
        if (m_buffers.size() == 0) return nullptr;

        return m_buffers[m_buffers.size() - 1];
    }

    /*
     * Return the number of buffers in this epoch at
     * time of call, and then clear the buffer vector,
     * releasing all references in the process.
     */
    size_t clear_buffers() {
        std::unique_lock<std::mutex> m_buffer_lock;
        size_t buf_cnt = m_buffers.size();
        for (auto buf : m_buffers) {
            if (buf) buf->release_reference();
        }

        m_buffers.clear();
        return buf_cnt;
    }

    /*
     * Returns a new Epoch object that is a copy of this one. The new object will also contain
     * a copy of the m_structure, rather than a reference to the same one. The epoch number of
     * the new epoch will be set to the provided argument.
     */
    Epoch *clone(size_t number) {
        std::unique_lock<std::mutex> m_buffer_lock;
        auto epoch = new Epoch(number);
        epoch->m_buffers = m_buffers;
        if (m_structure) {
            epoch->m_structure = m_structure->copy();
            /* the copy routine returns a structure with 0 references */
            epoch->m_structure->take_reference();
        }

        for (auto b : m_buffers) {
            b->take_reference();
        }

        return epoch;
    }

    /*
     * Check if a merge can be started from this Epoch.
     * At present, without concurrent merging, this simply
     * checks if there is currently a scheduled merge based
     * on this Epoch. If there is, returns false. If there
     * isn't, return true and set a flag indicating that
     * there is an active merge.
     */
    bool prepare_reconstruction() {
        auto old = m_active_merge.load();
        if (old) {
            return false;
        }

        // FIXME: this needs cleaned up
        while (!m_active_merge.compare_exchange_strong(old, true)) {
            old = m_active_merge.load();
            if (old) {
                return false;
            }
        }

        return true;
    }

    void set_inactive() {
        m_active = false;
    }

    /*
     * 
     */
    bool retirable() {
        /* if epoch is currently active, then it cannot be retired */
        if (m_active) {
            return false;
        }

        /* 
         * if the epoch has active jobs but is not itself active, 
         * wait for them to finish and return true. If there are 
         * not active jobs, return true immediately 
         */
        std::unique_lock<std::mutex> lk(m_cv_lock);
        while (m_active_jobs.load() > 0) {
            m_active_cv.wait(lk);
        }

        return true;
    }

private:
    Structure *m_structure;
    std::vector<Buffer *> m_buffers;

    std::condition_variable m_active_cv;
    std::mutex m_cv_lock;

    std::mutex m_buffer_lock;

    std::atomic<bool> m_active_merge;

    /*
     * The number of currently active jobs
     * (queries/merges) operating on this
     * epoch. An epoch can only be retired
     * when this number is 0.
     */
    std::atomic<size_t> m_active_jobs;
    bool m_active;
    size_t m_epoch_number;
};
}
