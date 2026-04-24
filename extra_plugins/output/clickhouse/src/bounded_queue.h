/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Bounded thread-safe queue (SPSC-optimised, single producer, single consumer)
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

/**
 * @brief Bounded thread-safe queue.
 *
 * - put()      blocks when capacity is reached (back-pressure to producer).
 * - try_put()  returns false immediately when full (non-blocking drop path).
 * - get()      blocks until an item is available.
 * - get(dur)   blocks for at most `dur`; returns nullopt on timeout.
 * - close()    wakes all waiters; subsequent get() returns nullopt.
 */
template <typename Item>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : m_capacity(capacity) {}

    // not copyable/movable — held by unique_ptr
    BoundedQueue(const BoundedQueue &) = delete;
    BoundedQueue &operator=(const BoundedQueue &) = delete;

    void put(Item item)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_space_cv.wait(lock, [this] { return m_closed || m_items.size() < m_capacity; });
        if (m_closed) return;
        m_items.push_back(std::move(item));
        m_avail_cv.notify_one();
    }

    bool try_put(Item item)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_closed || m_items.size() >= m_capacity) return false;
        m_items.push_back(std::move(item));
        m_avail_cv.notify_one();
        return true;
    }

    std::optional<Item> get()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_avail_cv.wait(lock, [this] { return m_closed || !m_items.empty(); });
        return pop_front_locked();
    }

    template <typename Rep, typename Period>
    std::optional<Item> get(std::chrono::duration<Rep, Period> timeout)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_avail_cv.wait_for(lock, timeout, [this] { return m_closed || !m_items.empty(); });
        return pop_front_locked();
    }

    void close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        m_avail_cv.notify_all();
        m_space_cv.notify_all();
    }

    std::size_t size() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_items.size();
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_avail_cv;
    std::condition_variable m_space_cv;
    std::deque<Item> m_items;
    std::size_t m_capacity;
    bool m_closed = false;

    std::optional<Item> pop_front_locked()
    {
        if (m_items.empty()) return std::nullopt;
        auto item = std::move(m_items.front());
        m_items.pop_front();
        m_space_cv.notify_one();
        return item;
    }
};
