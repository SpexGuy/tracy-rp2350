// This is an implementation of a single-producer, single-consumer byte stream.
// Each thread producing data can register one, and it can be drained by the
// profiler implementation.
//
// The bytestream ensures that "packets" of varying sizes are contiguous and
// do not wrap.

#pragma once

#include "../common/TracyAlign.hpp"
#include "../common/TracyYield.hpp"

#include <atomic>
#include <assert.h>
#include <limits>
#include <stdint.h>

#if defined(_MSC_VER) && !defined(__clang__)

#define BYTESTREAM_EXPECTED(x) (x)
#define BYTESTREAM_UNEXPECTED(x) (x)

#else

#define BYTESTREAM_EXPECTED(x) __builtin_expect((x), 1)
#define BYTESTREAM_UNEXPECTED(x) __builtin_expect((x), 0)

#endif

namespace tracy
{

struct BytestreamDefaultTraits
{
    typedef std::size_t index_t;

    typedef std::atomic<index_t> atomic_index_t;

    static bool default_idle() { YieldThread(); return true; }
};

template<typename Traits = BytestreamDefaultTraits>
struct Bytestream
{
    using Traits::index_t;
    using Traits::atomic_index_t;

    static_assert(atomic_index_t::is_always_lock_free, "Bytestream uses RMW ops that require lock free atomics");
    static_assert(!std::numeric_limits<index_t>::is_signed, "Bytestream::index_t must be unsigned!");
    constexpr index_t ABS_THREAD_TIME = static_cast<index_t>(1) << (sizeof(index_t) * CHAR_BIT - 1);
    constexpr index_t WRITE_INDEX_MASK = ~ABS_THREAD_TIME;

    Bytestream(char *data, index_t capacity)
        : m_data(data)
        , m_capacity(capacity)
    {
        assert(capacity & (capacity - 1) == 0); // Capacity must be power of two
        assert(capacity & WRITE_INDEX_MASK == capacity); // Capacity must not interfere with flags
    }

    /// Estimate the amount of data in the queue without interfering with writes
    index_t consumer_estimate_used_size() {
        const index_t read_pos = m_read_release.load(std::memory_order_relaxed);
        const index_t write_pos = m_write_commit_flags.load(std::memory_order_acquire) & WRITE_INDEX_MASK;
        if (read_pos <= write_pos) {
            return write_pos - read_pos;
        } else {
            // Use m_capacity instead of m_wrap because unfilled data at the end
            // still counts as unusable data and counts against the fill percent.
            return write_pos + m_capacity - read_pos;
        }
    }

    /// Determine where to stop reading. This is a guarantee that data will be read,
    /// and sets atomic state so that the writer uses absolute times for their next
    /// packet. However it does not yet mark the data as released.
    index_t consumer_snapshot_write_commit() {
        // Note: WriteContext::commit(..) assumes that this atomic or is the *only* operation that
        // the consumer will perform on write_commit_flags. If anything changes here, or more
        // modifications are added, commit will need to be updated.
        const index_t write_commit_flags = m_write_commit_flags.fetch_or(ABS_THREAD_TIME, std::memory_order_acq_rel);
        return write_commit_flags & WRITE_INDEX_MASK;
    }

    /// Safely consumes data from the queue, passing it to the data handler.
    /// DataHandler should be a function (char *ptr, index_t len) => void
    /// Each call to handler may contain a buffer of multiple packets,
    /// but packets will never be split across calls.
    template<typename DataHandler>
    index_t consume_data(DataHandler handler) {
        const index_t read_pos = m_read_release.load(std::memory_order_relaxed);
        const index_t write_pos = consumer_snapshot_write_commit();
        if (read_pos == write_pos) return 0;
        index_t consumed_bytes = 0;
        if (read_pos < write_pos) {
            consumed_bytes = write_pos - read_pos;
            handler(m_data + read_pos, consumed_bytes);
        } else {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            const index_t wrap = m_wrap.exchange(0, std::memory_order_acq_rel);
            assert(read_pos <= wrap);
#else
            const index_t wrap = m_wrap.load(std::memory_order_acquire);
#endif
            if (read_pos < wrap) {
                consumed_bytes += (wrap - read_pos);
                handler(m_data + read_pos, wrap - read_pos);
            }
            if (write_pos > 0) {
                if (read_pos < m_capacity) {
                    // Just in case the writer is blocking, publish the released space immediately.
                    m_read_release.store(m_capacity, std::memory_order_release);
                }
                consumed_bytes += write_pos;
                handler(m_data, write_pos);
            }
        }
        m_read_release.store(write_pos, std::memory_order_release);
    }

    WriteContext writer() {
        const index_t write_pos = m_write_commit_flags.load(std::memory_order_acquire) & WRITE_INDEX_MASK;
        return { this, write_pos };
    }

    struct WriteContext {
        Bytestream *m_stream;
        int64_t *m_time_ptr = nullptr;
        index_t m_write_pos;
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
        index_t m_reserved_pos;
        index_t m_total_reserved = 0;
        bool m_committed = false;
        bool m_moved = false;
        bool m_wrapped = false;
        bool m_allow_thread_time = false;
#endif

        WriteContext(Bytestream* stream, index_t write_pos)
            : m_stream(stream)
            , m_write_pos(write_pos)
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            , m_reserved_pos(write_pos)
#endif
        {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_stream->m_has_active_writer); // Stream cannot have multiple active writers
            m_stream->m_has_active_writer = true;
#endif
        }

        WriteContext(const WriteContext&) = delete;
        WriteContext& operator=(const WriteContext& other) = delete;
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
        WriteContext(WriteContext&& other) {
            memcpy(this, other, sizeof(WriteContext));
            other.m_moved = true;
        }
        WriteContext& operator=(WriteContext&& other) {
            assert(m_committed || m_moved); // Overwriting active context
            memcpy(this, other, sizeof(WriteContext));
            other.m_moved = true;
        }
#else
        WriteContext(WriteContext&&) = default;
        WriteContext& operator=(WriteContext&&) = default;
#endif

        /// Reserve space for a contiguous packet. This packet may not be contiguous
        /// with other reservations from the same writer. 
        tracy_force_inline bool try_reserve_space(index_t len) {
            return reserve_space_blocking(len, () => bool { return false; });
        }

        tracy_force_inline bool reserve_space_blocking(index_t len) {
            return reserve_space_blocking(len, Traits::default_idle);
        }

        template<typename BlockFn>
        tracy_force_inline bool reserve_space_blocking(index_t len, BlockFn idle) {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_moved); // Must not have been moved somewhere else
            assert(!m_committed); // Must not have committed already
            assert(m_write_pos == m_reserved_pos); // All reserved mem must be written
            assert(m_total_reserved + len < m_stream->m_capacity / 2); // Can't reserve too much space, would block indefinitely
#endif
            if (BYTESTREAM_UNEXPECTED(m_write_pos + len > m_stream->m_capacity)) {
                while (BYTESTREAM_UNEXPECTED(m_stream->m_read_release.load(std::memory_order_acquire) <= len)) {
                    if (!idle()) return false;
                }
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
                assert(!m_wrapped);
                m_wrapped = true;
                const index_t prev_wrap = m_stream->m_wrap.exchange(m_write_pos, std::memory_order_acq_rel);
                assert(prev_wrap == 0);
#else
                m_stream->m_wrap.store(m_write_pos, std::memory_order_release);
#endif
                m_write_pos = 0;
            } else {
                while (BYTESTREAM_UNEXPECTED(m_stream->m_read_release.load(std::memory_order_acquire) <= m_write_pos + len)) {
                    if (!idle()) return false;
                }
            }
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            m_reserved_pos = m_write_pos + len;
            m_total_reserved += len;
#endif
            return true;
        }

        tracy_force_inline void backoff_reservation(index_t num) {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_moved); // Must not have been moved somewhere else
            assert(!m_committed); // Must not have committed already
            assert(m_write_pos <= m_reserved_pos - num); // Must have reserved space to back off
            m_reserved_pos -= num;
            m_total_reserved -= num;
#endif
        }

        template<typename T>
        tracy_force_inline T* write_ptr() {
            return write_ptr_bytes<T>(sizeof(T));
        }

        template<typename T>
        tracy_force_inline T* write_ptr_num(index_t num) {
            return write_ptr_bytes<T>(num * sizeof(T))
        }

        template<typename T>
        tracy_force_inline T* write_ptr_bytes(index_t bytes) {
            T* const ptr = reinterpret_cast<T*>(m_stream->m_data + m_write_pos);
            m_write_pos += bytes;
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_moved); // Must not have been moved somewhere else
            assert(!m_committed); // Must not have committed already
            assert(m_write_pos <= m_reserved_pos); // Must have reserved space for the write
#endif
            return ptr;
        }

        tracy_force_inline void MemWriteThreadTime(int64_t* ptr, int64_t abs_time) {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_moved); // Must not have been moved somewhere else
            assert(!m_committed); // Must not have committed already
            assert(m_time_ptr == nullptr); // Only one thread time field allowed
            assert(m_allow_thread_time); // Need to declare TracySerialUseThreadContext
#endif
            m_time_type = type;
            m_time_ptr = ptr;
            // Encode the relative time by default
            int64_t ref_time = m_stream->m_refTimeThread;
            int64_t dt = abs_time - ref_time;
            m_stream->m_refTimeThread = abs_time;
            MemWrite(ptr, dt);
        }

        tracy_force_inline void commit() {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_moved); // Must not have been moved somewhere else
            assert(!m_committed); // Must not have committed already
            assert(m_stream->m_has_active_writer); // The stream should know about this writer
            m_stream->m_has_active_writer = false;
            m_committed = true;
            assert(m_write_pos == m_reserved_pos); // All reserved mem must be written
#endif

            index_t const time_bit = (m_time_ptr == nullptr) ? 0 : ABS_THREAD_TIME;
            index_t pos_flags = m_stream->m_write_commit_flags.load(std::memory_order_acquire);
            index_t const patched_flags = pos_flags & time_bit;
            if (BYTESTREAM_UNEXPECTED(patched_flags != 0)) {
                MemWrite<int64_t>(m_time_ptr, m_stream->m_refTimeThread);
            }
            index_t new_index_flags = (pos_flags & ABS_THREAD_TIME & ~time_bit) | m_write_pos;

            // If the consumer happened to read during this commit, we will see all pos_flags
            // set, which may differ from the previous values.
            if (BYTESTREAM_UNEXPECTED(!m_stream->m_write_commit_flags
                .compare_exchange_strong(pos_flags, new_index_flags, std::memory_order_acq_rel))) {
                index_t new_patch_flags = pos_flags & ~patched_flags & time_bit;
                if (new_patch_flags != 0) {
                    MemWrite<int64_t>(m_time_ptr, m_stream->m_refTimeThread);
                }

                // The other thread can only set bits, it can't change anything else.
                // If the above CAS failed, it must be because the other thread set
                // all the bits. So we know at this point that a compare and swap will
                // succeed, because all of the bits are set, and the consumer can't
                // set any more. It can no longer change the value. So we can safely
                // do an unconditional store.
                new_index_flags = (pos_flags & ABS_THREAD_TIME & ~time_bit) | m_write_pos;
                m_stream->m_write_commit_flags.store(new_index_flags, std::memory_order_release);
            }
        }

        tracy_force_inline void allow_thread_time() {
#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
            assert(!m_moved); // Must not have been moved somewhere else
            assert(!m_committed); // Must not have committed already
            assert(!m_allow_thread_time); // Only check thread once
            m_allow_thread_time = true;
#endif
        }

#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
        ~WriteContext() {
            assert(m_committed || m_moved); // WriteContexts must be committed, or moved and then committed
        }
#endif
    };

    char * const m_data;
    index_t const m_capacity;
    atomic_index_t m_wrap = 0;
    atomic_index_t m_write_commit_flags = 0;
    atomic_index_t m_read_release = 0;

    int64_t m_refTimeThread = 0;
    uint32_t m_threadId = -1;

#ifdef TRACY_BYTESTREAM_DEBUG_SAFETY
    bool m_has_active_writer = false;
#endif

    friend struct WriteContext;
};

}
