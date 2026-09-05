/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Statistics tracking and reporting
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#pragma once

#include "common.h"
#include <ipfixcol2.h>
#include <atomic>
#include <cstdint>
#include <ctime>

class Plugin; // Forward declaration of Plugin class.

/**
 * @class Stats
 * @brief A class for tracking and reporting statistics related to data processing.
 *
 * The Stats class is responsible for maintaining counters and timestamps
 * to track the progress of data processing and periodically printing
 * statistics in a throttled manner.
 */
class Stats {
public:
    /**
     * @brief Constructs a Stats object.
     *
     * @param ctx    Plugin context the metric series are registered under.
     * @param logger A Logger instance for logging statistics.
     * @param plugin A reference to the Plugin instance associated with this Stats object.
     */
    Stats(const ipx_ctx_t *ctx, Logger logger, Plugin& plugin);

    /**
     * @brief Adds a specified number of records to the processed count.
     *
     * @param count The number of records to add.
     */
    void add_recs(uint64_t count);

    /**
     * @brief Adds a specified number of rows to the written count.
     *
     * @param count The number of rows to add.
     */
    void add_rows(uint64_t count);

    /**
     * @brief Adds a specified number of records to the dropped count.
     *
     * @param count The number of records to add.
     */
    void add_dropped(uint64_t count);

    void add_enqueue_drop(uint64_t count = 1);

    void add_template_broadcast(uint64_t count = 1);

    /**
     * @brief Counts one field conversion error.
     * @return true while the caller is still expected to log it (the first few only).
     */
    bool add_conversion_error();

    /** @brief Counts one successful insert of @p rows rows that took @p millis ms. */
    void add_insert(uint64_t rows, uint64_t millis);

    /** @brief Counts one failed insert attempt (the inserter retries it). */
    void add_insert_error();

    /**
     * @brief Prints the statistics if sufficient time has passed since the last print.
     */
    void print_stats_throttled(time_t now);

private:
    Logger m_logger; ///< Logger instance for logging statistics.
    Plugin &m_plugin; ///< Reference to the associated Plugin instance.
    std::atomic<uint64_t> m_recs_processed_since_last{0}; ///< Records processed since the last statistics print.
    std::atomic<uint64_t> m_conversion_errors{0}; ///< Field conversion errors seen (drives the log cap).
    /// Registry series; the totals live only here (see ipfixcol2/metrics.h).
    struct {
        ipx_metric_t *recs, *rows, *dropped, *enqueue_drops, *template_broadcasts, *conversion_errors;
        ipx_metric_t *inserts, *insert_errors, *rows_inserted, *insert_millis;
        ipx_metric_t *blocks_avail, *blocks_filled, *blocks_total, *proc_queue, *proc_queue_max;
    } m_m{};
    time_t m_start_time = 0; ///< Start time of the statistics tracking.
    time_t m_last_stats_print_time = 0; ///< Time of the last statistics print.
};
