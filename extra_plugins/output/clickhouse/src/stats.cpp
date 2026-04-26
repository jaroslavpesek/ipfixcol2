/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Statistics tracking and reporting
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "stats.h"
#include "plugin.h"

#include <algorithm>
#include <limits>

static constexpr int STATS_PRINT_INTERVAL_SECS = 1;

Stats::Stats(Logger logger, Plugin& plugin) : m_logger(logger), m_plugin(plugin) {}

void Stats::add_recs(uint64_t count)
{
    m_recs_processed_since_last.fetch_add(count, std::memory_order_relaxed);
    m_recs_processed_total.fetch_add(count, std::memory_order_relaxed);
}

void Stats::add_rows(uint64_t count)
{
    m_rows_written_total.fetch_add(count, std::memory_order_relaxed);
}

void Stats::add_dropped(uint64_t count)
{
    m_recs_dropped_total.fetch_add(count, std::memory_order_relaxed);
}

void Stats::add_enqueue_drop(uint64_t count)
{
    m_enqueue_drops_total.fetch_add(count, std::memory_order_relaxed);
}

void Stats::add_template_broadcast(uint64_t count)
{
    m_template_broadcasts_total.fetch_add(count, std::memory_order_relaxed);
}

void Stats::print_stats_throttled(time_t now)
{
    if (m_start_time == 0) {
        m_start_time = now;
    }

    if ((now - m_last_stats_print_time) > STATS_PRINT_INTERVAL_SECS) {
        double total_rps = m_recs_processed_total.load(std::memory_order_relaxed) / std::max<double>(1, now - m_start_time);
        double immediate_rps = m_recs_processed_since_last.load(std::memory_order_relaxed) / std::max<double>(1, now - m_last_stats_print_time);

        std::size_t proc_q_total = 0;
        std::size_t proc_q_max = 0;
        for (const auto &queue : m_plugin.m_proc_queues) {
            std::size_t size = queue->size();
            proc_q_total += size;
            proc_q_max = std::max(proc_q_max, size);
        }

        uint64_t proc_rec_min = 0;
        uint64_t proc_rec_max = 0;
        if (!m_plugin.m_proc_record_counts.empty()) {
            proc_rec_min = std::numeric_limits<uint64_t>::max();
            for (uint64_t count : m_plugin.m_proc_record_counts) {
                proc_rec_min = std::min(proc_rec_min, count);
                proc_rec_max = std::max(proc_rec_max, count);
            }
        }

        m_logger.info("STATS - RECS: %lu (%lu dropped), ROWS: %lu, AVG: %.2f recs/sec, AVG_IMMEDIATE: %.2f recs/sec, BLK_AVAIL_Q: %lu, BLK_FILL_Q: %lu, PROC_Q_TOTAL: %lu, PROC_Q_MAX: %lu, PROC_REC_MIN: %lu, PROC_REC_MAX: %lu, TMPLT_BCASTS: %lu, ENQ_DROPS: %lu",
                      m_recs_processed_total.load(std::memory_order_relaxed),
                      m_recs_dropped_total.load(std::memory_order_relaxed),
                      m_rows_written_total.load(std::memory_order_relaxed),
                      total_rps,
                      immediate_rps,
                      m_plugin.m_avail_blocks.size(),
                      m_plugin.m_filled_blocks.size(),
                      proc_q_total,
                      proc_q_max,
                      proc_rec_min,
                      proc_rec_max,
                      m_template_broadcasts_total.load(std::memory_order_relaxed),
                      m_enqueue_drops_total.load(std::memory_order_relaxed)
                      );
        m_recs_processed_since_last.store(0, std::memory_order_relaxed);
        m_last_stats_print_time = now;
    }
}
