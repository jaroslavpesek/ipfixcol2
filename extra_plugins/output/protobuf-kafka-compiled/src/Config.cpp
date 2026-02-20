/**
 * \file Config.cpp
 * \brief Configuration parser for protobuf-kafka-compiled output plugin
 */

#include "Config.hpp"

namespace protobuf_kafka_compiled {

// XML node identifiers
enum XmlNodes {
  NODE_BROKERS = 1,
  NODE_TOPIC,
  NODE_PARTITION,
  NODE_BATCH_SIZE,
  NODE_LINGER_MS,
  NODE_COMPRESSION,
  NODE_BLOCKING,
  NODE_MAP,
  NODE_FIELD,
  ATTR_IPFIX,
  ATTR_PROTO
};

// <field>
static const struct fds_xml_args args_field[] = {
    FDS_OPTS_ATTR(ATTR_IPFIX, "ipfix", FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ATTR(ATTR_PROTO, "proto", FDS_OPTS_T_STRING, 0), FDS_OPTS_END};

// <map>
static const struct fds_xml_args args_map[] = {
    FDS_OPTS_NESTED(NODE_FIELD, "field", args_field, FDS_OPTS_P_MULTI),
    FDS_OPTS_END};

// <params>
static const struct fds_xml_args args_params[] = {
    FDS_OPTS_ROOT("params"),
    FDS_OPTS_ELEM(NODE_BROKERS, "brokers", FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ELEM(NODE_TOPIC, "topic", FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ELEM(NODE_PARTITION, "partition", FDS_OPTS_T_STRING,
                  FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_BATCH_SIZE, "batch_size", FDS_OPTS_T_UINT,
                  FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_LINGER_MS, "linger_ms", FDS_OPTS_T_UINT, FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_COMPRESSION, "compression", FDS_OPTS_T_STRING,
                  FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_BLOCKING, "blocking", FDS_OPTS_T_BOOL, FDS_OPTS_P_OPT),
    FDS_OPTS_NESTED(NODE_MAP, "map", args_map, 0),
    FDS_OPTS_END};

/**
 * \brief Parse IPFIX element specification
 *
 * \param[in]  spec   IPFIX specification string
 * \param[in]  iemgr  Information Element manager
 * \param[out] pen    Parsed PEN
 * \param[out] id     Parsed element ID
 * \return true on success, false if element not found
 */
static bool parse_ipfix_spec(const char *spec, const fds_iemgr_t *iemgr,
                             uint32_t &pen, uint16_t &id) {
  const fds_iemgr_elem *elem = fds_iemgr_elem_find_name(iemgr, spec);
  if (!elem) {
    return false;
  }

  pen = elem->scope->pen;
  id = elem->id;
  return true;
}

/**
 * \brief Parse <field> element
 */
static FieldMapping parse_field(fds_xml_ctx_t *ctx, const fds_iemgr_t *iemgr) {
  FieldMapping mapping{};
  std::string ipfix_spec;

  const struct fds_xml_cont *content;
  while (fds_xml_next(ctx, &content) != FDS_EOC) {
    switch (content->id) {
    case ATTR_IPFIX:
      ipfix_spec = content->ptr_string;
      break;
    case ATTR_PROTO:
      mapping.proto_name = content->ptr_string;
      break;
    default:
      throw std::runtime_error("Unexpected attribute in <field>");
    }
  }

  if (ipfix_spec.empty()) {
    throw std::runtime_error("<field> missing 'ipfix' attribute");
  }
  if (mapping.proto_name.empty()) {
    throw std::runtime_error("<field> missing 'proto' attribute");
  }

  if (!parse_ipfix_spec(ipfix_spec.c_str(), iemgr, mapping.ipfix_pen,
                        mapping.ipfix_id)) {
    throw std::runtime_error("Unknown IPFIX element: " + ipfix_spec);
  }

  mapping.ipfix_spec = ipfix_spec;
  return mapping;
}

/**
 * \brief Parse <map> element
 */
static void parse_map(fds_xml_ctx_t *ctx, const fds_iemgr_t *iemgr,
                      std::vector<FieldMapping> &mappings) {
  const struct fds_xml_cont *content;
  while (fds_xml_next(ctx, &content) != FDS_EOC) {
    if (content->id == NODE_FIELD) {
      mappings.push_back(parse_field(content->ptr_ctx, iemgr));
    } else {
      throw std::runtime_error("Unexpected element in <map>");
    }
  }
}

Config parse_config(const char *params, const fds_iemgr_t *iemgr,
                    ipx_ctx_t *ctx) {
  std::string partMode;
  Config cfg;
  cfg.partition_mode = PartitionMode::RANDOM;

  std::unique_ptr<fds_xml_t, decltype(&fds_xml_destroy)> xml(fds_xml_create(),
                                                             &fds_xml_destroy);
  if (!xml) {
    throw std::runtime_error("Failed to create XML parser");
  }

  if (fds_xml_set_args(xml.get(), args_params) != FDS_OK) {
    throw std::runtime_error("Failed to set XML parser arguments");
  }

  fds_xml_ctx_t *params_ctx = fds_xml_parse_mem(xml.get(), params, true);
  if (!params_ctx) {
    std::string err = fds_xml_last_err(xml.get());
    throw std::runtime_error("Failed to parse configuration: " + err);
  }

  const struct fds_xml_cont *content;
  while (fds_xml_next(params_ctx, &content) != FDS_EOC) {
    switch (content->id) {
    case NODE_BROKERS:
      cfg.brokers = content->ptr_string;
      break;

    case NODE_TOPIC:
      cfg.topic = content->ptr_string;
      break;

    case NODE_PARTITION:
      if (strcasecmp(content->ptr_string, "random") == 0) {
        cfg.partition_mode = PartitionMode::RANDOM;
        partMode = "random";
      } else if (strcasecmp(content->ptr_string, "rss") == 0) {
        cfg.partition_mode = PartitionMode::RSS;
        partMode = "rss";
      } else if (strcasecmp(content->ptr_string, "precomputed_rss") == 0) {
        cfg.partition_mode = PartitionMode::PRECOMPUTED_RSS;
        partMode = "precomputed_rss";
      } else {
        throw std::runtime_error(
            "Invalid partition mode: " + std::string(content->ptr_string) +
            " (expected 'random', 'rss', or 'precomputed_rss')");
      }
      break;

    case NODE_BATCH_SIZE:
      cfg.batch_size = static_cast<uint32_t>(content->val_uint);
      break;

    case NODE_LINGER_MS:
      cfg.linger_ms = static_cast<uint32_t>(content->val_uint);
      break;

    case NODE_COMPRESSION:
      cfg.compression = content->ptr_string;
      break;

    case NODE_BLOCKING:
      cfg.blocking = content->val_bool;
      break;

    case NODE_MAP:
      parse_map(content->ptr_ctx, iemgr, cfg.mappings);
      break;

    default:
      throw std::runtime_error("Unknown configuration element");
    }
  }

  // Validate required fields
  if (cfg.brokers.empty()) {
    throw std::runtime_error("<brokers> is required");
  }
  if (cfg.topic.empty()) {
    throw std::runtime_error("<topic> is required");
  }
  if (cfg.mappings.empty()) {
    throw std::runtime_error(
        "At least one <field> mapping is required in <map>");
  }

  IPX_CTX_INFO(ctx, "Configuration: brokers=%s, topic=%s, partition=%s",
               cfg.brokers.c_str(), cfg.topic.c_str(), partMode);
  IPX_CTX_INFO(ctx, "Field mappings: %zu configured", cfg.mappings.size());

  return cfg;
}

} // namespace protobuf_kafka_compiled
