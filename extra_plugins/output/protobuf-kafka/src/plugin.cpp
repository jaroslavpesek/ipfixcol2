/**
 * \file plugin.cpp
 * \brief IPFIXcol2 protobuf-kafka output plugin
 * \author Jaroslav Pesek
 * \date 2026
 *
 * This plugin serializes IPFIX flow records into Protocol Buffers and sends
 * them to a Kafka topic. It uses dynamic schema loading.
 */

#include <ipfixcol2.h>
#include <libfds.h>

#include <memory>
#include <stdexcept>

#include "Config.hpp"
#include "ProtoSchema.hpp"
#include "TranslationTable.hpp"
#include "KafkaProducer.hpp"
#include "FlowConverter.hpp"

namespace {

/**
 * \brief Plugin instance context
 */
struct PluginContext {
    std::unique_ptr<protobuf_kafka::Config> config;
    std::unique_ptr<protobuf_kafka::ProtoSchema> schema;
    std::unique_ptr<protobuf_kafka::TranslationTable> table;
    std::unique_ptr<protobuf_kafka::KafkaProducer> kafka;
    std::unique_ptr<protobuf_kafka::FlowConverter> converter;
};

static int32_t
computePartition(const PluginContext& data, const protobuf_kafka::PartitionKey& key)
{
    if (data.config->partition_mode != protobuf_kafka::PartitionMode::RSS || !key.valid) {
        return RD_KAFKA_PARTITION_UA;
    }

    if (key.has_flow_id) {
        return protobuf_kafka::KafkaProducer::computeRssPartitionFromFlowId(
            key.flow_id,
            data.kafka->partitionCount());
    }

    return protobuf_kafka::KafkaProducer::computeRssPartition(
        key.src_ip, key.src_ip_len,
        key.dst_ip, key.dst_ip_len,
        key.src_port, key.dst_port,
        key.protocol,
        data.kafka->partitionCount());
}

static void
processRecord(PluginContext& data, struct ipx_ipfix_record* rec)
{
    if (!rec || rec->rec.tmplt == nullptr || rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) {
        return;
    }

    const char* buf = nullptr;
    size_t len = 0;
    protobuf_kafka::PartitionKey key{};

    if (!data.converter->convert(&rec->rec, &buf, &len, &key)) {
        return;
    }

    data.kafka->produce(buf, len, computePartition(data, key));
}

} // anonymous namespace

/// Plugin identification structure
IPX_API struct ipx_plugin_info ipx_plugin_info = {
    // Plugin identification name
    "protobuf-kafka",
    // Brief description of plugin
    "Serializes IPFIX records to Protocol Buffers and sends to Kafka",
    // Plugin type
    IPX_PT_OUTPUT,
    // Configuration flags - use DEEPBIND for protobuf library compatibility
    IPX_PF_DEEPBIND,
    // Plugin version string
    "1.0.0",
    // Minimal IPFIXcol version string
    "2.2.0"
};

/**
 * \brief Plugin initialization
 */
extern "C" IPX_API int
ipx_plugin_init(ipx_ctx_t* ctx, const char* params)
{
    try {
        IPX_CTX_INFO(ctx, "Initializing protobuf-kafka output plugin...");

        auto data = std::make_unique<PluginContext>();

        const fds_iemgr_t* iemgr = ipx_ctx_iemgr_get(ctx);
        if (!iemgr) {
            IPX_CTX_ERROR(ctx, "Failed to get Information Element manager");
            return IPX_ERR_DENIED;
        }

        data->config = std::make_unique<protobuf_kafka::Config>(
            protobuf_kafka::parse_config(params, iemgr, ctx));

        IPX_CTX_INFO(ctx, "Loading proto file: %s", data->config->proto_file.c_str());
        data->schema = std::make_unique<protobuf_kafka::ProtoSchema>(
            data->config->proto_file, data->config->message_type);

        data->table = std::make_unique<protobuf_kafka::TranslationTable>();
        data->table->build(data->config->mappings, *data->schema, iemgr, ctx);

        data->kafka = std::make_unique<protobuf_kafka::KafkaProducer>(
            *data->config, ctx);

        data->converter = std::make_unique<protobuf_kafka::FlowConverter>(
            *data->schema, *data->table, data->config->partition_mode);

        IPX_CTX_INFO(ctx, "Protobuf-kafka plugin initialized successfully");

        ipx_ctx_private_set(ctx, data.release());
        return IPX_OK;

    } catch (const std::exception& ex) {
        IPX_CTX_ERROR(ctx, "Initialization failed: %s", ex.what());
        return IPX_ERR_DENIED;
    } catch (...) {
        IPX_CTX_ERROR(ctx, "Initialization failed: unknown exception");
        return IPX_ERR_DENIED;
    }
}

/**
 * \brief Plugin destruction
 */
extern "C" IPX_API void
ipx_plugin_destroy(ipx_ctx_t* ctx, void* cfg)
{
    if (cfg == nullptr) {
        return;
    }

    auto* data = static_cast<PluginContext*>(cfg);
    delete data;

    IPX_CTX_INFO(ctx, "Protobuf-kafka plugin destroyed");
}

/**
 * \brief Process IPFIX message
 */
extern "C" IPX_API int
ipx_plugin_process(ipx_ctx_t* ctx, void* cfg, ipx_msg_t* msg)
{
    (void)ctx;

    auto* data = static_cast<PluginContext*>(cfg);
    ipx_msg_ipfix_t* ipfix_msg = ipx_msg_base2ipfix(msg);

    const uint32_t rec_cnt = ipx_msg_ipfix_get_drec_cnt(ipfix_msg);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        struct ipx_ipfix_record* rec = ipx_msg_ipfix_get_drec(ipfix_msg, i);
        processRecord(*data, rec);
    }

    return IPX_OK;
}
