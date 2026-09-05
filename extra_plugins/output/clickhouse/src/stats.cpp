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
/// Conversion errors logged per plugin instance before they are only counted.
static constexpr uint64_t CONVERSION_ERROR_LOG_LIMIT = 100;

Stats::Stats(const ipx_ctx_t *ctx, Logger logger, Plugin& plugin) : m_logger(logger), m_plugin(plugin)
{
    auto counter = [ctx](const char *name, const char *help) {
        return ipx_ctx_metric_new(ctx, IPX_METRIC_COUNTER, name, help, nullptr, 0);
    };
    auto gauge = [ctx](const char *name, const char *help) {
        return ipx_ctx_metric_new(ctx, IPX_METRIC_GAUGE, name, help, nullptr, 0);
    };
    m_m.recs = counter("ipfixcol2_clickhouse_records_total", "Data records received by the plugin");
    m_m.rows = counter("ipfixcol2_clickhouse_rows_total", "Rows built into insert blocks");
    m_m.dropped = counter("ipfixcol2_clickhouse_records_dropped_total", "Data records dropped because no block was free (nonblocking mode)");
    m_m.enqueue_drops = counter("ipfixcol2_clickhouse_enqueue_drops_total", "Messages dropped because a processor queue was full (nonblocking mode)");
    m_m.template_broadcasts = counter("ipfixcol2_clickhouse_template_broadcasts_total", "Template register items sent to processors");
    m_m.conversion_errors = counter("ipfixcol2_clickhouse_conversion_errors_total", "Field conversion errors (the value is stored as NULL/default)");
    m_m.inserts = counter("ipfixcol2_clickhouse_inserts_total", "Blocks inserted into ClickHouse");
    m_m.insert_errors = counter("ipfixcol2_clickhouse_insert_errors_total", "Failed insert attempts (each is retried)");
    m_m.rows_inserted = counter("ipfixcol2_clickhouse_rows_inserted_total", "Rows acknowledged by ClickHouse");
    m_m.insert_millis = counter("ipfixcol2_clickhouse_insert_milliseconds_total", "Wall time spent in successful inserts");
    m_m.blocks_avail = gauge("ipfixcol2_clickhouse_blocks_available", "Free blocks");
    m_m.blocks_filled = gauge("ipfixcol2_clickhouse_blocks_filled", "Blocks waiting for an inserter");
    m_m.blocks_total = gauge("ipfixcol2_clickhouse_blocks_total", "Configured block count");
    m_m.proc_queue = gauge("ipfixcol2_clickhouse_processor_queue_items", "Items queued for processor threads (all queues)");
    m_m.proc_queue_max = gauge("ipfixcol2_clickhouse_processor_queue_items_max", "Items queued for the busiest processor thread");
}

void Stats::add_recs(uint64_t count)
{
    m_recs_processed_since_last.fetch_add(count, std::memory_order_relaxed);
    ipx_metric_add(m_m.recs, count);
}

void Stats::add_rows(uint64_t count)
{
    ipx_metric_add(m_m.rows, count);
}

void Stats::add_dropped(uint64_t count)
{
    ipx_metric_add(m_m.dropped, count);
}

void Stats::add_enqueue_drop(uint64_t count)
{
    ipx_metric_add(m_m.enqueue_drops, count);
}

void Stats::add_template_broadcast(uint64_t count)
{
    ipx_metric_add(m_m.template_broadcasts, count);
}

bool Stats::add_conversion_error()
{
    ipx_metric_add(m_m.conversion_errors, 1);
    uint64_t seen = m_conversion_errors.fetch_add(1, std::memory_order_relaxed);
    if (seen == CONVERSION_ERROR_LOG_LIMIT) {
        m_logger.warning("Reached %lu field conversion errors; further ones are only counted "
                         "(ipfixcol2_clickhouse_conversion_errors_total)", seen);
    }
    return seen < CONVERSION_ERROR_LOG_LIMIT;
}

void Stats::add_insert(uint64_t rows, uint64_t millis)
{
    ipx_metric_add(m_m.inserts, 1);
    ipx_metric_add(m_m.rows_inserted, rows);
    ipx_metric_add(m_m.insert_millis, millis);
}

void Stats::add_insert_error()
{
    ipx_metric_add(m_m.insert_errors, 1);
}

void Stats::print_stats_throttled(time_t now)
{
    if (m_start_time == 0) {
        m_start_time = now;
    }

    if ((now - m_last_stats_print_time) > STATS_PRINT_INTERVAL_SECS) {
        double total_rps = ipx_metric_get(m_m.recs) / std::max<double>(1, now - m_start_time);
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

        ipx_metric_set(m_m.blocks_avail, m_plugin.m_avail_blocks.size());
        ipx_metric_set(m_m.blocks_filled, m_plugin.m_filled_blocks.size());
        ipx_metric_set(m_m.blocks_total, m_plugin.m_blocks.size());
        ipx_metric_set(m_m.proc_queue, proc_q_total);
        ipx_metric_set(m_m.proc_queue_max, proc_q_max);

        m_logger.info("STATS - RECS: %lu (%lu dropped), ROWS: %lu, AVG: %.2f recs/sec, AVG_IMMEDIATE: %.2f recs/sec, BLK_AVAIL_Q: %lu, BLK_FILL_Q: %lu, PROC_Q_TOTAL: %lu, PROC_Q_MAX: %lu, PROC_REC_MIN: %lu, PROC_REC_MAX: %lu, TMPLT_BCASTS: %lu, ENQ_DROPS: %lu",
                      ipx_metric_get(m_m.recs),
                      ipx_metric_get(m_m.dropped),
                      ipx_metric_get(m_m.rows),
                      total_rps,
                      immediate_rps,
                      m_plugin.m_avail_blocks.size(),
                      m_plugin.m_filled_blocks.size(),
                      proc_q_total,
                      proc_q_max,
                      proc_rec_min,
                      proc_rec_max,
                      ipx_metric_get(m_m.template_broadcasts),
                      ipx_metric_get(m_m.enqueue_drops)
                      );
        m_recs_processed_since_last.store(0, std::memory_order_relaxed);
        m_last_stats_print_time = now;
    }
}
