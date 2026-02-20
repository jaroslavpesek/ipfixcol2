/**
 * \file Config.hpp
 * \brief Configuration parser for protobuf-kafka-compiled output plugin
 */

#ifndef PROTOBUF_KAFKA_COMPILED_CONFIG_HPP
#define PROTOBUF_KAFKA_COMPILED_CONFIG_HPP

#include <cstdint>
#include <cstring>
#include <ipfixcol2.h>
#include <libfds.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace protobuf_kafka_compiled {

/**
 * \brief Partition mode for Kafka producer
 */
enum class PartitionMode {
  RANDOM,         ///< Random partition assignment (RD_KAFKA_PARTITION_UA)
  RSS,            ///< Receiver-side scaling based on 5-tuple hash
  PRECOMPUTED_RSS ///< Receiver-side scaling based on value handed over FLOW_ID
                  ///< element
};

/**
 * \brief Mapping between IPFIX field and Protobuf field
 */
struct FieldMapping {
  uint32_t ipfix_pen;     ///< IPFIX Private Enterprise Number
  uint16_t ipfix_id;      ///< IPFIX Information Element ID
  std::string proto_name; ///< Protobuf field name
  std::string ipfix_spec; ///< Original IPFIX specification (for error messages)
};

/**
 * \brief Plugin configuration
 */
struct Config {
  std::string brokers;          ///< Kafka broker list (comma-separated)
  std::string topic;            ///< Kafka topic name
  PartitionMode partition_mode; ///< Partition assignment mode
  uint32_t batch_size = 10000;  ///< batch.num.messages
  uint32_t linger_ms = 100;     ///< queue.buffering.max.ms
  std::string compression =
      "lz4";             ///< compression.codec (none, gzip, snappy, lz4, zstd)
  bool blocking = false; ///< Block when queue is full
  std::string message_type;           ///< Fully qualified message type name
  std::vector<FieldMapping> mappings; ///< IPFIX to Protobuf field mappings
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
Config parse_config(const char *params, const fds_iemgr_t *iemgr,
                    ipx_ctx_t *ctx);

} // namespace protobuf_kafka_compiled

#endif // PROTOBUF_KAFKA_COMPILED_CONFIG_HPP
