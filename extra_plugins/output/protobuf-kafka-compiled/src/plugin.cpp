/**
 * \file plugin.cpp
 * \brief IPFIXcol2 protobuf-kafka-compiled output plugin
 */

#include "../generated_schema/schema.pb.h" // generated protobuf class (command protoc --cpp_out=. schema)
#include "Config.hpp"
#include "FlowConverter.hpp"
#include "KafkaProducer.hpp"
#include "TranslationTable.hpp"

#include <ipfixcol2.h>
#include <libfds.h>
#include <memory>
#include <stdexcept>

namespace {
struct PluginContext {
  std::unique_ptr<protobuf_kafka_compiled::Config> config;
  std::unique_ptr<protobuf_kafka_compiled::TranslationTable> table;
  std::unique_ptr<protobuf_kafka_compiled::KafkaProducer> kafka;
  std::unique_ptr<protobuf_kafka_compiled::FlowConverter> converter;
};
} // anonymous namespace

IPX_API struct ipx_plugin_info ipx_plugin_info = {
    "protobuf-kafka-compiled",
    "Serializes IPFIX records to Protocol Buffers and sends to Kafka",
    IPX_PT_OUTPUT,
    IPX_PF_DEEPBIND,
    "1.0.0",
    "2.2.0"};

extern "C" IPX_API int ipx_plugin_init(ipx_ctx_t *ctx, const char *params) {
  try {
    IPX_CTX_INFO(ctx, "Initializing protobuf-kafka output plugin...");

    auto data = std::make_unique<PluginContext>();

    const fds_iemgr_t *iemgr = ipx_ctx_iemgr_get(ctx);
    if (!iemgr) {
      IPX_CTX_ERROR(ctx, "Failed to get Information Element manager");
      return IPX_ERR_DENIED;
    }

    // Parse configuration
    data->config = std::make_unique<protobuf_kafka_compiled::Config>(
        protobuf_kafka_compiled::parse_config(params, iemgr, ctx));

    IPX_CTX_INFO(ctx, "Using compiled protobuf: ipfix::FlowRecord");

    // Build translation table (from map created during config initialization)
    data->table = std::make_unique<protobuf_kafka_compiled::TranslationTable>();
    data->table->build(data->config->mappings, iemgr, ctx);

    // Kafka producer
    data->kafka = std::make_unique<protobuf_kafka_compiled::KafkaProducer>(
        *data->config, ctx);

    // Flow converter
    data->converter = std::make_unique<protobuf_kafka_compiled::FlowConverter>(
        *data->table, data->config->partition_mode);

    ipx_ctx_private_set(ctx, data.release());

    IPX_CTX_INFO(ctx, "Protobuf-kafka plugin initialized successfully");
    return IPX_OK;

  } catch (const std::exception &ex) {
    IPX_CTX_ERROR(ctx, "Initialization failed: %s", ex.what());
    return IPX_ERR_DENIED;
  } catch (...) {
    IPX_CTX_ERROR(ctx, "Initialization failed: unknown exception");
    return IPX_ERR_DENIED;
  }
}

extern "C" IPX_API void ipx_plugin_destroy(ipx_ctx_t *ctx, void *cfg) {
  auto *data = static_cast<PluginContext *>(cfg);
  delete data;

  IPX_CTX_INFO(ctx, "Protobuf-kafka plugin destroyed");
}

extern "C" IPX_API int ipx_plugin_process(ipx_ctx_t *ctx, void *cfg,
                                          ipx_msg_t *msg) {
  (void)ctx;

  auto *data = static_cast<PluginContext *>(cfg);
  ipx_msg_ipfix_t *ipfix_msg = ipx_msg_base2ipfix(msg);

  const uint32_t rec_cnt = ipx_msg_ipfix_get_drec_cnt(ipfix_msg);

  for (uint32_t i = 0; i < rec_cnt; ++i) {

    struct ipx_ipfix_record *rec = ipx_msg_ipfix_get_drec(ipfix_msg, i);

    // skip options templates
    if (rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS)
      continue;

    ipfix::FlowRecord record;
    protobuf_kafka_compiled::PartitionKey pk{};

    if (!data->converter->convert(&rec->rec, record, &pk))
      continue;

    // serialize protobuf
    std::string buffer;
    if (!record.SerializeToString(&buffer))
      continue;

    int32_t partition = RD_KAFKA_PARTITION_UA;

    if (data->config->partition_mode ==
            protobuf_kafka_compiled::PartitionMode::RSS &&
        pk.valid) {
      partition = protobuf_kafka_compiled::KafkaProducer::computeRssPartition(
          pk.src_ip, pk.src_ip_len, pk.dst_ip, pk.dst_ip_len, pk.src_port,
          pk.dst_port, pk.protocol, data->kafka->partitionCount());
    } else if (data->config->partition_mode ==
                   protobuf_kafka_compiled::PartitionMode::PRECOMPUTED_RSS &&
               pk.valid) {
      partition = pk.flowID;
    }

    data->kafka->produce(buffer.data(), buffer.size(), partition);
  }

  return IPX_OK;
}
