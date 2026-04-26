/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Main plugin class
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#pragma once

#include "block.h"
#include "bounded_queue.h"
#include "clickhouse.h"
#include "column.h"
#include "common.h"
#include "config.h"
#include "inserter.h"
#include "processor.h"
#include "recparser.h"
#include "stats.h"

#include <ipfixcol2.h>
#include <libfds.h>
#include <memory>
#include <unordered_map>


/**
 * @class Plugin
 * @brief Class representing the ClickHouse output plugin
 */
class Plugin : Nonmoveable, Noncopyable {
public:
    /**
     * @brief Instantiate the plugin instance
     *
     * @param ctx The collector context for logging
     * @param xml_config The config in form of a XML string
     */
    Plugin(ipx_ctx_t *ctx, const char *xml_config);

    /**
     * @brief Process a collector message
     *
     * @param msg The collector message
     */
    void process(ipx_msg_t *msg);

    /**
     * @brief Stop the plugin and wait till it is stopped (blocking)
     */
    void stop();

private:
    friend class Stats;

    Logger m_logger;
    Config m_config;
    const fds_iemgr_t *m_iemgr = nullptr;
    std::vector<Column> m_columns;

    std::vector<std::unique_ptr<Block>> m_blocks;

    std::vector<std::unique_ptr<Inserter>> m_inserters;

    SyncQueue<Block *> m_avail_blocks;
    SyncQueue<Block *> m_filled_blocks;

    // --- Inline (legacy) path state ---
    Block *m_current_block = nullptr;
    std::time_t m_last_insert_time = 0;
    std::unique_ptr<RecParserManager> m_rec_parsers;

    // --- Parallel processor path state ---
    std::vector<std::unique_ptr<Processor>> m_processors;
    std::vector<std::unique_ptr<BoundedQueue<ProcItem>>> m_proc_queues;

    // Tracks the last template generation delivered by the dispatcher.
    struct TemplateKey {
        const ipx_session *session;
        uint32_t           odid;
        uint16_t           tmpl_id;
        bool operator==(const TemplateKey &o) const noexcept {
            return session == o.session && odid == o.odid && tmpl_id == o.tmpl_id;
        }
    };
    struct TemplateKeyHash {
        std::size_t operator()(const TemplateKey &k) const noexcept {
            std::size_t h = std::hash<const void *>{}(k.session);
            h ^= std::hash<uint32_t>{}(k.odid)    + 0x9e3779b9u + (h << 6) + (h >> 2);
            h ^= std::hash<uint16_t>{}(k.tmpl_id) + 0x9e3779b9u + (h << 6) + (h >> 2);
            return h;
        }
    };
    std::unordered_map<TemplateKey, FdsTemplatePtr, TemplateKeyHash> m_known_templates;

    std::vector<uint64_t> m_proc_record_counts;
    std::size_t m_next_proc_queue = 0;

    Stats m_stats;

    void
    process_ipfix_msg(ipx_msg_ipfix_t *msg);

    void
    process_ipfix_msg_parallel(ipx_msg_ipfix_t *msg);

    void
    process_session_msg(ipx_msg_session_t *msg);

    std::size_t
    select_processor_shard(const ipx_session *session);

    bool
    enqueue_proc_item(std::size_t shard, ProcItem item);

    bool
    register_template(const ipx_session *session, uint32_t odid, const fds_template *tmplt, std::size_t shard);

    int
    process_record(ipx_msg_ipfix_t *msg, fds_drec &rec, Block &block);

    void
    extract_values(ipx_msg_ipfix_t *msg, RecParser &parser, Block &block, bool rev);
};
