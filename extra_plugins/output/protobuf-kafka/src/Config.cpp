/**
 * \file Config.cpp
 * \brief Configuration parser for protobuf-kafka output plugin
 * \author Jaroslav Pesek
 * \date 2026
 */

#include "Config.hpp"

#include <libfds.h>
#include <ipfixcol2.h>
#include <stdexcept>
#include <memory>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <limits>

namespace protobuf_kafka {

// XML node identifiers
enum XmlNodes {
    NODE_BROKERS = 1,
    NODE_TOPIC,
    NODE_PARTITION,
    NODE_BATCH_SIZE,
    NODE_LINGER_MS,
    NODE_COMPRESSION,
    NODE_BLOCKING,
    NODE_PROTO_FILE,
    NODE_MESSAGE_TYPE,
    NODE_MAP,
    NODE_FIELD,
    ATTR_IPFIX,
    ATTR_PROTO
};

// <field>
static const struct fds_xml_args args_field[] = {
    FDS_OPTS_ATTR(ATTR_IPFIX, "ipfix", FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ATTR(ATTR_PROTO, "proto", FDS_OPTS_T_STRING, 0),
    FDS_OPTS_END
};

// <map>
static const struct fds_xml_args args_map[] = {
    FDS_OPTS_NESTED(NODE_FIELD, "field", args_field, FDS_OPTS_P_MULTI),
    FDS_OPTS_END
};

// <params>
static const struct fds_xml_args args_params[] = {
    FDS_OPTS_ROOT("params"),
    FDS_OPTS_ELEM(NODE_BROKERS,      "brokers",      FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ELEM(NODE_TOPIC,        "topic",        FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ELEM(NODE_PARTITION,    "partition",    FDS_OPTS_T_STRING, FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_BATCH_SIZE,   "batch_size",   FDS_OPTS_T_UINT,   FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_LINGER_MS,    "linger_ms",    FDS_OPTS_T_UINT,   FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_COMPRESSION,  "compression",  FDS_OPTS_T_STRING, FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_BLOCKING,     "blocking",     FDS_OPTS_T_BOOL,   FDS_OPTS_P_OPT),
    FDS_OPTS_ELEM(NODE_PROTO_FILE,   "proto_file",   FDS_OPTS_T_STRING, 0),
    FDS_OPTS_ELEM(NODE_MESSAGE_TYPE, "message_type", FDS_OPTS_T_STRING, 0),
    FDS_OPTS_NESTED(NODE_MAP,        "map",          args_map,          0),
    FDS_OPTS_END
};

/**
 * \brief Trim leading/trailing ASCII whitespace
 */
static std::string
trim_copy(const std::string& input)
{
    size_t begin = 0;
    while (begin < input.size() &&
           std::isspace(static_cast<unsigned char>(input[begin])) != 0) {
        ++begin;
    }

    size_t end = input.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }

    return input.substr(begin, end - begin);
}

/**
 * \brief Parse numeric IPFIX element specification in "e<pen>id<id>" format
 */
static bool
parse_numeric_ipfix_spec(const std::string& spec, uint32_t& pen, uint16_t& id)
{
    if (spec.size() < 5U || (spec[0] != 'e' && spec[0] != 'E')) {
        return false;
    }

    char* end_ptr = nullptr;
    errno = 0;
    unsigned long parsed_pen = std::strtoul(spec.c_str() + 1, &end_ptr, 10);
    if (errno != 0 || end_ptr == spec.c_str() + 1 || end_ptr == nullptr ||
        parsed_pen > std::numeric_limits<uint32_t>::max()) {
        return false;
    }

    if (end_ptr == nullptr || *end_ptr == '\0' || *(end_ptr + 1) == '\0') {
        return false;
    }
    if ((end_ptr[0] != 'i' && end_ptr[0] != 'I') ||
        (end_ptr[1] != 'd' && end_ptr[1] != 'D')) {
        return false;
    }

    char* id_end = nullptr;
    errno = 0;
    unsigned long parsed_id = std::strtoul(end_ptr + 2, &id_end, 10);
    if (errno != 0 || id_end == end_ptr + 2 || id_end == nullptr || *id_end != '\0' ||
        parsed_id > std::numeric_limits<uint16_t>::max()) {
        return false;
    }

    pen = static_cast<uint32_t>(parsed_pen);
    id = static_cast<uint16_t>(parsed_id);
    return true;
}

/**
 * \brief Parse one IPFIX token (name or "e<pen>id<id>")
 *
 * \param[in]  token  Token to parse
 * \param[in]  iemgr  Information Element manager
 * \param[out] pen    Parsed PEN
 * \param[out] id     Parsed element ID
 * \return true on success
 */
static bool
parse_ipfix_token(const std::string& token, const fds_iemgr_t* iemgr,
                  uint32_t& pen, uint16_t& id)
{
    const std::string normalized = trim_copy(token);
    if (normalized.empty()) {
        return false;
    }

    if (parse_numeric_ipfix_spec(normalized, pen, id)) {
        return true;
    }

    const fds_iemgr_elem* elem = fds_iemgr_elem_find_name(iemgr, normalized.c_str());
    if (!elem) {
        return false;
    }

    pen = elem->scope->pen;
    id = elem->id;
    return true;
}

/**
 * \brief Parse an IPFIX specification
 *
 * Supports:
 * - single field: "scope:name" or "e<pen>id<id>"
 * - basicList selector: "<root>/<list_elem>"
 *
 * \param[in]  spec      IPFIX specification string
 * \param[in]  iemgr  Information Element manager
 * \param[out] mapping Parsed mapping record
 * \return true on success, false if element not found
 */
static bool
parse_ipfix_spec(const std::string& spec, const fds_iemgr_t* iemgr,
                 FieldMapping& mapping)
{
    const std::string normalized = trim_copy(spec);
    if (normalized.empty()) {
        return false;
    }

    const size_t slash_pos = normalized.find('/');
    if (slash_pos == std::string::npos) {
        return parse_ipfix_token(normalized, iemgr, mapping.root_pen, mapping.root_id);
    }

    if (normalized.find('/', slash_pos + 1) != std::string::npos) {
        return false;
    }

    const std::string root_token = normalized.substr(0, slash_pos);
    const std::string child_token = normalized.substr(slash_pos + 1);
    if (!parse_ipfix_token(root_token, iemgr, mapping.root_pen, mapping.root_id)) {
        return false;
    }
    if (!parse_ipfix_token(child_token, iemgr, mapping.list_pen, mapping.list_id)) {
        return false;
    }

    mapping.is_list = true;
    return true;
}

/**
 * \brief Parse <field> element
 */
static FieldMapping
parse_field(fds_xml_ctx_t* ctx, const fds_iemgr_t* iemgr)
{
    FieldMapping mapping{};
    std::string ipfix_spec;

    const struct fds_xml_cont* content;
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

    // Special keyword: "odid" maps to Observation Domain ID from message context
    if (strcasecmp(trim_copy(ipfix_spec).c_str(), "odid") == 0) {
        mapping.is_odid = true;
        mapping.ipfix_spec = "odid";
        return mapping;
    }

    if (!parse_ipfix_spec(ipfix_spec, iemgr, mapping)) {
        throw std::runtime_error("Unknown or invalid IPFIX element specification: " + ipfix_spec);
    }

    mapping.ipfix_spec = ipfix_spec;
    return mapping;
}

/**
 * \brief Parse <map> element
 */
static void
parse_map(fds_xml_ctx_t* ctx, const fds_iemgr_t* iemgr,
          std::vector<FieldMapping>& mappings)
{
    const struct fds_xml_cont* content;
    while (fds_xml_next(ctx, &content) != FDS_EOC) {
        if (content->id == NODE_FIELD) {
            mappings.push_back(parse_field(content->ptr_ctx, iemgr));
        } else {
            throw std::runtime_error("Unexpected element in <map>");
        }
    }
}

Config
parse_config(const char* params, const fds_iemgr_t* iemgr, ipx_ctx_t* ctx)
{
    Config cfg;
    cfg.partition_mode = PartitionMode::RANDOM;

    std::unique_ptr<fds_xml_t, decltype(&fds_xml_destroy)>
        xml(fds_xml_create(), &fds_xml_destroy);
    if (!xml) {
        throw std::runtime_error("Failed to create XML parser");
    }

    if (fds_xml_set_args(xml.get(), args_params) != FDS_OK) {
        throw std::runtime_error("Failed to set XML parser arguments");
    }

    fds_xml_ctx_t* params_ctx = fds_xml_parse_mem(xml.get(), params, true);
    if (!params_ctx) {
        std::string err = fds_xml_last_err(xml.get());
        throw std::runtime_error("Failed to parse configuration: " + err);
    }

    const struct fds_xml_cont* content;
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
            } else if (strcasecmp(content->ptr_string, "rss") == 0) {
                cfg.partition_mode = PartitionMode::RSS;
            } else {
                throw std::runtime_error(
                    "Invalid partition mode: " + std::string(content->ptr_string) +
                    " (expected 'random' or 'rss')");
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

        case NODE_PROTO_FILE:
            cfg.proto_file = content->ptr_string;
            break;

        case NODE_MESSAGE_TYPE:
            cfg.message_type = content->ptr_string;
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
    if (cfg.proto_file.empty()) {
        throw std::runtime_error("<proto_file> is required");
    }
    if (cfg.message_type.empty()) {
        throw std::runtime_error("<message_type> is required");
    }
    if (cfg.mappings.empty()) {
        throw std::runtime_error("At least one <field> mapping is required in <map>");
    }
    IPX_CTX_INFO(ctx, "Configuration: brokers=%s, topic=%s, partition=%s",
                 cfg.brokers.c_str(), cfg.topic.c_str(),
                 cfg.partition_mode == PartitionMode::RSS ? "rss" : "random");
    IPX_CTX_INFO(ctx, "Proto file: %s, message type: %s",
                 cfg.proto_file.c_str(), cfg.message_type.c_str());
    IPX_CTX_INFO(ctx, "Field mappings: %zu configured", cfg.mappings.size());

    return cfg;
}

} // namespace protobuf_kafka
