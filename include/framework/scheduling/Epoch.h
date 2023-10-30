/*
 * include/framework/scheduling/Epoch.h
 *
 * Copyright (C) 2023 Douglas B. Rumbaugh <drumbaugh@psu.edu> 
 *                    Dong Xie <dongx@psu.edu>
 *
 * All rights reserved. Published under the Modified BSD License.
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
    Epoch()
        : m_buffers()
        , m_structure(nullptr)
        , m_active_jobs(0) 
    {}

    Epoch(Structure *structure, Buffer *buff) 
        : m_buffers()
        , m_structure(structure)
        , m_active_jobs(0) 
    {
        m_buffers.push_back(buff);
    }

    ~Epoch() {
        assert(m_active_jobs.load() == 0);

        for (auto buf : m_buffers) {
            buf->release_reference();
        }

        if (m_structure) {
            m_structure->release_reference();
        }
    }

    void add_buffer(Buffer *buf) {
        assert(buf);

        buf->take_reference();
        m_buffers.push_back(buf);
    }

    void start_job() {
        m_active_jobs.fetch_add(1);
    }

    void end_job() {
        m_active_jobs.fetch_add(-1);
    }

    size_t get_active_job_num() {
        return m_active_jobs.load();
    }

    Structure *get_structure() {
        return m_structure;
    }

    std::vector<Buffer *> &get_buffers() {
        return m_buffers;
    }

    BufView get_buffer_view() {
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
        size_t buf_cnt = m_buffers.size();
        for (auto buf : m_buffers) {
            if (buf) buf->release_reference();
        }

        m_buffers.clear();
        return buf_cnt;
    }

    /*
     * Returns a new Epoch object that is a copy of this one. The new object will also contain
     * a copy of the m_structure, rather than a reference to the same one.
     */
    Epoch *clone() {
        auto epoch = new Epoch();
        epoch->m_buffers = m_buffers;
        if (m_structure) {
            epoch->m_structure = m_structure->copy();
        }

        return epoch;
    }

private:
    Structure *m_structure;
    std::vector<Buffer *> m_buffers;

    /*
     * The number of currently active jobs
     * (queries/merges) operating on this
     * epoch. An epoch can only be retired
     * when this number is 0.
     */
    std::atomic<size_t> m_active_jobs;
};
}
