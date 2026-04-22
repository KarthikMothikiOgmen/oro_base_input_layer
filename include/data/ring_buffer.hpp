#ifndef DATA_RING_BUFFER_HPP
#define DATA_RING_BUFFER_HPP
// ============================================================================
// ring_buffer.hpp — Lock-Free SPSC Ring Buffer
//
// Single-producer, single-consumer (SPSC) ring buffer for zero-contention
// inter-thread communication. Uses std::atomic with acquire/release memory
// ordering — no mutexes, no locks, no syscalls in the hot path.
//
// Usage:
//   - Serial I/O thread (producer) pushes raw bytes into rx_queue
//   - Main decode thread (consumer) pops bytes for packet assembly
//
// Template parameters:
//   T    — element type (typically uint8_t for byte streams)
//   N    — buffer capacity (must be power of 2 for modular arithmetic)
// ============================================================================

#include <atomic>
#include <cstddef>
#include <cstring>
#include <type_traits>

namespace oro {

template <typename T, size_t N>
class RingBuffer {
    static_assert((N & (N - 1)) == 0, "RingBuffer capacity must be a power of 2");
    static_assert(std::is_trivially_copyable_v<T>, "RingBuffer requires trivially copyable types");

public:
    RingBuffer() = default;

    // Non-copyable, non-movable
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    // ── Single-element operations ───────────────────────────────────────

    // Producer: try to push a single element. Returns false if full.
    bool try_push(const T& item) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & MASK;

        if (next == tail_.load(std::memory_order_acquire)) {
            return false;  // Buffer full
        }

        buffer_[head] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    // Consumer: try to pop a single element. Returns false if empty.
    bool try_pop(T& item) {
        const size_t tail = tail_.load(std::memory_order_relaxed);

        if (tail == head_.load(std::memory_order_acquire)) {
            return false;  // Buffer empty
        }

        item = buffer_[tail];
        tail_.store((tail + 1) & MASK, std::memory_order_release);
        return true;
    }

    // ── Bulk operations ─────────────────────────────────────────────────

    // Producer: push up to 'count' elements. Returns number actually pushed.
    size_t push_bulk(const T* data, size_t count) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t tail = tail_.load(std::memory_order_acquire);
        const size_t available = (tail - head - 1) & MASK;

        if (available == 0) return 0;

        const size_t to_push = (count < available) ? count : available;

        // Copy in up to two segments (wrap-around)
        const size_t first_chunk = N - (head & MASK);
        if (to_push <= first_chunk) {
            std::memcpy(&buffer_[head & MASK], data, to_push * sizeof(T));
        } else {
            std::memcpy(&buffer_[head & MASK], data, first_chunk * sizeof(T));
            std::memcpy(&buffer_[0], data + first_chunk, (to_push - first_chunk) * sizeof(T));
        }

        head_.store((head + to_push) & MASK, std::memory_order_release);
        return to_push;
    }

    // Consumer: pop up to 'max_count' elements. Returns number actually popped.
    size_t pop_bulk(T* data, size_t max_count) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        const size_t head = head_.load(std::memory_order_acquire);
        const size_t available = (head - tail) & MASK;

        if (available == 0) return 0;

        const size_t to_pop = (max_count < available) ? max_count : available;

        // Copy out in up to two segments (wrap-around)
        const size_t first_chunk = N - (tail & MASK);
        if (to_pop <= first_chunk) {
            std::memcpy(data, &buffer_[tail & MASK], to_pop * sizeof(T));
        } else {
            std::memcpy(data, &buffer_[tail & MASK], first_chunk * sizeof(T));
            std::memcpy(data + first_chunk, &buffer_[0], (to_pop - first_chunk) * sizeof(T));
        }

        tail_.store((tail + to_pop) & MASK, std::memory_order_release);
        return to_pop;
    }

    // ── Query ───────────────────────────────────────────────────────────

    size_t size() const {
        return (head_.load(std::memory_order_acquire) -
                tail_.load(std::memory_order_acquire)) & MASK;
    }

    bool empty() const { return size() == 0; }
    bool full() const  { return size() == (N - 1); }

    static constexpr size_t capacity() { return N - 1; }  // One slot reserved

private:
    static constexpr size_t MASK = N - 1;

    // Cache-line padding to prevent false sharing between producer and consumer
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) T buffer_[N]{};
};

}  // namespace oro
#endif // DATA_RING_BUFFER_HPP
