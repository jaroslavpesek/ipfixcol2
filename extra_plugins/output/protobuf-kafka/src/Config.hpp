/**
 * \file Config.hpp
 * \brief Configuration parser for protobuf-kafka output plugin
 * \author Jaroslav Pesek
 * \date 2026
 */

#ifndef PROTOBUF_KAFKA_CONFIG_HPP
#define PROTOBUF_KAFKA_CONFIG_HPP

#include <string>
#include <vector>
#include <cstdint>

#include <ipfixcol2.h>
#include <libfds.h>

namespace protobuf_kafka {

/**
 * \brief Partition mode for Kafka producer
 */
enum class PartitionMode {
    RANDOM,  ///< Random partition assignment (RD_KAFKA_PARTITION_UA)
    RSS      ///< Receiver-side scaling based on flowId (fallback to 5-tuple hash)
};

/**
 * \brief Mapping between IPFIX field and Protobuf field
 */
struct FieldMapping {
    uint32_t root_pen = 0;      ///< Root IPFIX PEN (field itself)
    uint16_t root_id = 0;       ///< Root IPFIX IE ID (field itself)
    bool is_list = false;       ///< True when mapping points to a basicList element
    uint32_t list_pen = 0;      ///< basicList element PEN (valid when is_list=true)
    uint16_t list_id = 0;       ///< basicList element IE ID (valid when is_list=true)
    bool is_odid = false;       ///< True when field is filled from ODID (not IPFIX data)
    std::string proto_name;  ///< Protobuf field name
    std::string ipfix_spec;  ///< Original IPFIX specification (for error messages)
};

/**
 * \brief Plugin configuration
 */
struct Config {
    std::string brokers;                  ///< Kafka broker list (comma-separated)
    std::string topic;                    ///< Kafka topic name
    PartitionMode partition_mode;         ///< Partition assignment mode
    uint32_t batch_size = 10000;          ///< batch.num.messages
    uint32_t linger_ms = 100;             ///< queue.buffering.max.ms
    std::string compression = "lz4";      ///< compression.codec (none, gzip, snappy, lz4, zstd)
    bool blocking = false;                ///< Block when queue is full
    std::string proto_file;               ///< Path to .proto file
    std::string message_type;             ///< Fully qualified message type name
    std::vector<FieldMapping> mappings;   ///< IPFIX to Protobuf field mappings
};

/**
 * \brief Parse XML configuration
 *
 * \param[in] params  XML configuration string
 * \param[in] iemgr   Information Element manager for resolving IPFIX names
 * \param[in] ctx     Plugin context for logging
 * \return Parsed configuration
 * \throws std::runtime_error on parse error
 */
Config
parse_config(const char* params, const fds_iemgr_t* iemgr, ipx_ctx_t* ctx);

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_CONFIG_HPP
