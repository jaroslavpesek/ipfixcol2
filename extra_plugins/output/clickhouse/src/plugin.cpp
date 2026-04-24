/**
 * @file
 * @author Michal Sedlak <sedlakm@cesnet.cz>
 * @brief Main plugin class
 * @date 2025
 *
 * Copyright(c) 2025 CESNET z.s.p.o.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "datatype.h"
#include "plugin.h"

#include <cassert>
#include <ipfixcol2.h>
#include <libfds.h>

static std::vector<Column> prepare_columns(std::vector<Config::Column> &columns_cfg)
{
    std::vector<Column> columns;

    for (const auto &column_cfg : columns_cfg) {
        Column column{};
        DataType type;

        if (std::holds_alternative<const fds_iemgr_elem *>(column_cfg.source)) {
            const fds_iemgr_elem *elem = std::get<const fds_iemgr_elem *>(column_cfg.source);

            if (elem->data_type == FDS_ET_BASIC_LIST) {
                if (!column_cfg.inner_elem) {
                    throw Error("column \"{}\" source is a basicList element but <innerSource> is not specified", column_cfg.name);
                }
                type = type_from_ipfix(column_cfg.inner_elem->data_type);
                column.is_list = true;
                column.inner_elem = column_cfg.inner_elem;
            } else {
                type = type_from_ipfix(elem->data_type);
            }

            column.elem = elem;

        } else if (std::holds_alternative<const fds_iemgr_alias *>(column_cfg.source)) {
            const fds_iemgr_alias *alias = std::get<const fds_iemgr_alias *>(column_cfg.source);
            type = find_common_type(*alias);
            column.alias = alias;

        } else /* if (std::holds_alternative<SpecialField>(column_cfg.source)) */ {
            type = DataType::UInt32;
            column.special = std::get<SpecialField>(column_cfg.source);

        }

        column.name = column_cfg.name;
        column.datatype = type;
        column.nullable = column_cfg.nullable;

        columns.emplace_back(std::move(column));
    }

    return columns;
}


Plugin::Plugin(ipx_ctx_t *ctx, const char *xml_config)
    : m_logger(ctx)
    , m_stats(m_logger, *this)
{
    // Subscribe to periodic messages as well to ensure data export even when no data is coming
    ipx_msg_mask_t new_mask = IPX_MSG_IPFIX | IPX_MSG_PERIODIC | IPX_MSG_SESSION;
    int rc = ipx_ctx_subscribe(ctx, &new_mask, nullptr);
    if (rc != IPX_OK) {
        throw Error("ipx_ctx_subscribe() failed with error code {}", rc);
    }

    // Parse config
    m_iemgr = ipx_ctx_iemgr_get(ctx);
    m_config = parse_config(xml_config, m_iemgr);

    std::vector<clickhouse::Endpoint> endpoints;
    for (const Config::Endpoint &endpoint_cfg : m_config.connection.endpoints) {
        endpoints.push_back(clickhouse::Endpoint{endpoint_cfg.host, endpoint_cfg.port});
    }

    m_columns = prepare_columns(m_config.columns);

    // Ensure we have enough blocks for all processors + inserters + some slack.
    uint64_t n_proc = m_config.processor_threads;
    uint64_t needed_blocks = (n_proc > 0)
        ? std::max(m_config.blocks, 2 * (n_proc + m_config.inserter_threads))
        : m_config.blocks;
    if (needed_blocks > m_config.blocks) {
        m_logger.warning("Bumping blocks from %lu to %lu to avoid pool starvation (processorThreads=%lu, inserterThreads=%lu)",
                         m_config.blocks, needed_blocks, n_proc, m_config.inserter_threads);
    }

    // Prepare blocks
    for (unsigned int i = 0; i < needed_blocks; i++) {
        std::unique_ptr<Block> blk = std::make_unique<Block>();
        blk->list_scratch.resize(m_columns.size());
        for (std::size_t ci = 0; ci < m_columns.size(); ci++) {
            blk->columns.emplace_back(make_column(m_columns[ci].datatype, m_columns[ci].nullable, m_columns[ci].is_list));
            blk->block.AppendColumn(m_columns[ci].name, blk->columns.back());
            if (m_columns[ci].is_list) {
                blk->list_scratch[ci] = make_column(m_columns[ci].datatype, m_columns[ci].nullable, false);
            }
        }
        m_blocks.emplace_back(std::move(blk));
        m_avail_blocks.put(m_blocks.back().get());
    }

    // Prepare inserters
    for (unsigned int i = 0; i < m_config.inserter_threads; i++) {
        clickhouse::ClientOptions client_opts = clickhouse::ClientOptions()
            .SetEndpoints(endpoints)
            .SetUser(m_config.connection.user)
            .SetPassword(m_config.connection.password)
            .SetDefaultDatabase(m_config.connection.database);
        std::unique_ptr<Inserter> ins = std::make_unique<Inserter>(
            i,
            m_logger,
            client_opts,
            m_config.connection.table,
            m_columns,
            m_filled_blocks,
            m_avail_blocks);

        m_inserters.emplace_back(std::move(ins));
    }

    // Prepare parallel processors (if enabled)
    if (n_proc > 0) {
        for (unsigned int i = 0; i < n_proc; i++) {
            m_proc_queues.push_back(
                std::make_unique<BoundedQueue<ProcItem>>(m_config.processor_queue_depth));
        }

        for (unsigned int i = 0; i < n_proc; i++) {
            m_processors.push_back(std::make_unique<Processor>(
                i,
                m_logger,
                m_columns,
                m_iemgr,
                m_stats,
                m_avail_blocks,
                m_filled_blocks,
                m_config.biflow_empty_autoignore,
                m_config.block_insert_threshold,
                m_config.block_insert_max_delay_secs,
                *m_proc_queues[i]));
        }
    } else {
        // Legacy inline path
        m_rec_parsers = std::make_unique<RecParserManager>(m_columns, m_config.biflow_empty_autoignore);
    }

    m_logger.info("Starting inserters");
    for (auto &ins : m_inserters) {
        ins->start();
    }

    if (n_proc > 0) {
        m_logger.info("Starting %lu processor threads", n_proc);
        for (auto &proc : m_processors) {
            proc->start();
        }
    }

    m_logger.info("ClickHouse plugin is ready");
}

// ---------------------------------------------------------------------------
// Inline (legacy, processor_threads==0) path — unchanged logic.
// ---------------------------------------------------------------------------

void Plugin::extract_values(ipx_msg_ipfix_t *msg, RecParser &parser, Block &block, bool rev)
{
    std::size_t n_columns = m_columns.size();
    bool has_value;
    ValueVariant value;

    for (std::size_t i = 0; i < n_columns; i++) {
        if (m_columns[i].is_list) {
            fds_drec_field &field = parser.get_column(i, rev);
            if (field.data != nullptr) {
                try {
                    write_list_to_column(m_columns[i].datatype, m_columns[i].nullable,
                                         field, *block.columns[i].get(), m_iemgr,
                                         block.list_scratch[i]);
                } catch (const ConversionError& err) {
                    m_logger.error("List field conversion failed (field #%zu, \"%s\"): %s",
                                   i, m_columns[i].name.c_str(), err.what());
                    write_empty_list_to_column(*block.columns[i].get(), block.list_scratch[i]);
                }
            } else {
                write_empty_list_to_column(*block.columns[i].get(), block.list_scratch[i]);
            }
            continue;
        }

        has_value = false;

        if (m_columns[i].special == SpecialField::ODID) {
            value = ipx_msg_ipfix_get_ctx(msg)->odid;
            has_value = true;

        } else {
            fds_drec_field &field = parser.get_column(i, rev);
            if (field.data != nullptr) {
                try {
                    value = get_value(m_columns[i].datatype, field);
                    has_value = true;
                } catch (const ConversionError& err) {
                    m_logger.error("Field conversion failed (field #%zu, \"%s\"): %s",
                                   i, m_columns[i].name.c_str(), err.what());
                }
            }
        }

        write_to_column(m_columns[i].datatype, m_columns[i].nullable, *block.columns[i].get(), has_value ? &value : nullptr);
    }
    block.rows++;
}

int
Plugin::process_record(ipx_msg_ipfix_t *msg, fds_drec &rec, Block &block)
{
    if (rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) {
        return 0;
    }

    int ret = 0;
    RecParser &parser = m_rec_parsers->get_parser(rec.tmplt);
    parser.parse_record(rec);

    if (!parser.skip_fwd()) {
        extract_values(msg, parser, block, false);
        ret++;
    }

    if (!parser.skip_rev()) {
        extract_values(msg, parser, block, true);
        ret++;
    }

    return ret;
}

void
Plugin::process_session_msg(ipx_msg_session_t *msg)
{
    if (ipx_msg_session_get_event(msg) != IPX_MSG_SESSION_CLOSE) return;

    const ipx_session *sess = ipx_msg_session_get_session(msg);

    if (!m_processors.empty()) {
        // Parallel path: remove from known-template set, then FIFO-deliver to owner worker.
        for (auto it = m_known_templates.begin(); it != m_known_templates.end(); ) {
            if (it->session == sess) {
                it = m_known_templates.erase(it);
            } else {
                ++it;
            }
        }
        // Route to owning worker (same hash as used for WorkItems).
        std::size_t shard = std::hash<const void *>{}(sess) % m_proc_queues.size();
        m_proc_queues[shard]->put(ProcItem{SessionClose{sess}});
    } else {
        m_rec_parsers->delete_session(sess);
    }
}

void
Plugin::process_ipfix_msg(ipx_msg_ipfix_t *msg)
{
    // Inline (legacy) path: no processor threads.
    if (m_current_block == nullptr) {
        if (m_config.nonblocking) {
            std::optional<Block *> maybe_block = m_avail_blocks.try_get();
            if (maybe_block.has_value()) {
                m_current_block = maybe_block.value();
            } else {
                uint32_t drec_cnt = ipx_msg_ipfix_get_drec_cnt(msg);
                m_stats.add_dropped(drec_cnt);
                return;
            }
        } else {
            m_current_block = m_avail_blocks.get();
        }
    }

    const ipx_msg_ctx *msg_ctx = ipx_msg_ipfix_get_ctx(msg);
    if (msg_ctx->session->type == FDS_SESSION_SCTP) {
        throw std::runtime_error("SCTP is not supported at this time");
    }
    m_rec_parsers->select_session(msg_ctx->session);
    m_rec_parsers->select_odid(msg_ctx->odid);

    uint32_t drec_cnt = ipx_msg_ipfix_get_drec_cnt(msg);
    uint32_t rows_count = 0;
    for (uint32_t idx = 0; idx < drec_cnt; idx++) {
        ipx_ipfix_record *rec = ipx_msg_ipfix_get_drec(msg, idx);
        uint32_t rows_inserted = process_record(msg, rec->rec, *m_current_block);
        rows_count += rows_inserted;
    }

    m_stats.add_recs(drec_cnt);
    m_stats.add_rows(rows_count);
}

void
Plugin::process_ipfix_msg_parallel(ipx_msg_ipfix_t *msg)
{
    // Parallel path: dispatcher thread deep-copies drec data and enqueues to workers.
    const ipx_msg_ctx *msg_ctx = ipx_msg_ipfix_get_ctx(msg);
    if (msg_ctx->session->type == FDS_SESSION_SCTP) {
        throw std::runtime_error("SCTP is not supported at this time");
    }

    const ipx_session *session = msg_ctx->session;
    uint32_t odid = msg_ctx->odid;
    uint32_t drec_cnt = ipx_msg_ipfix_get_drec_cnt(msg);
    std::size_t shard = std::hash<const void *>{}(session) % m_proc_queues.size();
    auto &queue = *m_proc_queues[shard];

    m_stats.add_recs(drec_cnt);

    // Pass 1: ensure worker knows all templates in this message (before enqueuing WorkItem).
    for (uint32_t idx = 0; idx < drec_cnt; idx++) {
        ipx_ipfix_record *rec = ipx_msg_ipfix_get_drec(msg, idx);
        if (rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) continue;

        TemplateKey key{session, odid, rec->rec.tmplt->id};
        if (m_known_templates.find(key) == m_known_templates.end()) {
            fds_template *copy = fds_template_copy(rec->rec.tmplt);
            if (!copy) throw std::bad_alloc{};
            queue.put(ProcItem{TemplateRegister{session, odid, rec->rec.tmplt->id, copy}});
            m_known_templates.insert(key);
        }
    }

    // Pass 2: pack drec field data into a single owned buffer.
    uint32_t total_size = 0;
    for (uint32_t idx = 0; idx < drec_cnt; idx++) {
        ipx_ipfix_record *rec = ipx_msg_ipfix_get_drec(msg, idx);
        if (rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) continue;
        total_size += rec->rec.size;
    }

    if (total_size == 0) return; // all records were opts — nothing to dispatch

    std::unique_ptr<uint8_t[]> buf(new uint8_t[total_size]);
    std::vector<DrecInfo> drecs;
    drecs.reserve(drec_cnt);

    uint32_t offset = 0;
    for (uint32_t idx = 0; idx < drec_cnt; idx++) {
        ipx_ipfix_record *rec = ipx_msg_ipfix_get_drec(msg, idx);
        if (rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) continue;
        std::memcpy(buf.get() + offset, rec->rec.data, rec->rec.size);
        drecs.push_back({rec->rec.tmplt->id, offset, rec->rec.size});
        offset += rec->rec.size;
    }

    WorkItem wi;
    wi.buf     = std::move(buf);
    wi.drecs   = std::move(drecs);
    wi.session = session;
    wi.odid    = odid;

    if (m_config.nonblocking) {
        if (!queue.try_put(ProcItem{std::move(wi)})) {
            m_stats.add_dropped(drec_cnt);
        }
    } else {
        queue.put(ProcItem{std::move(wi)});
    }
}

void
Plugin::process(ipx_msg_t *msg)
{
    if (ipx_msg_get_type(msg) == IPX_MSG_SESSION) {
        process_session_msg(ipx_msg_base2session(msg));

    } else if (ipx_msg_get_type(msg) == IPX_MSG_IPFIX) {
        ipx_msg_ipfix_t *ipfix_msg = ipx_msg_base2ipfix(msg);
        if (!m_processors.empty()) {
            process_ipfix_msg_parallel(ipfix_msg);
        } else {
            process_ipfix_msg(ipfix_msg);
        }
    }

    time_t now = std::time(nullptr);

    if (m_processors.empty()) {
        // Inline path: flush logic unchanged.
        if (m_current_block) {
            bool nonempty = m_current_block->rows > 0;
            bool thresh_reached = m_current_block->rows >= m_config.block_insert_threshold;
            bool timeout_reached = uint64_t(now - m_last_insert_time) >= m_config.block_insert_max_delay_secs;

            if (nonempty && (thresh_reached || timeout_reached)) {
                m_filled_blocks.put(m_current_block);
                m_current_block = nullptr;
                m_last_insert_time = now;
            }
        }
    } else {
        // Parallel path: broadcast PeriodicFlush to all workers once per timeout period.
        if (uint64_t(now - m_last_insert_time) >= m_config.block_insert_max_delay_secs) {
            for (auto &q : m_proc_queues) {
                q->put(ProcItem{PeriodicFlush{}});
            }
            m_last_insert_time = now;
        }
    }

    // Print stats
    m_stats.print_stats_throttled(now);

    // Check for any exceptions thrown by workers
    for (auto &ins : m_inserters) {
        ins->check_error();
    }
    for (auto &proc : m_processors) {
        proc->check_error();
    }
}

void Plugin::stop()
{
    if (!m_processors.empty()) {
        // Parallel path: stop processors first (FIFO drain ensures all WorkItems are processed).
        m_logger.info("Sending stop signal to processor threads...");
        for (auto &q : m_proc_queues) {
            q->put(ProcItem{Stop{}});
        }
        m_logger.info("Waiting for processor threads to finish...");
        for (auto &proc : m_processors) {
            proc->join();
        }
    } else {
        // Inline path: export what's left in the last block.
        if (m_current_block && m_current_block->rows > 0) {
            m_filled_blocks.put(m_current_block);
            m_current_block = nullptr;
        }
    }

    // Stop all inserter threads and wait for them to finish.
    m_logger.info("Sending stop signal to inserter threads...");
    for (auto &ins : m_inserters) {
        ins->request_stop();
    }
    for (const auto &ins : m_inserters) {
        (void) ins;
        m_filled_blocks.put(nullptr);
    }

    m_logger.info("Waiting for inserter threads to finish...");
    for (auto &ins : m_inserters) {
        ins->join();
    }

    std::size_t drop_count = 0;
    for (const auto &block : m_blocks) {
        drop_count += block->rows;
    }
    m_logger.warning("%zu rows could not have been inserted and have been dropped due to termination timeout",
                     drop_count);
}
