/**
 * @file KafkaProducer.hpp
 * @brief Kafka producer wrapper
 * @author Jaroslav Pesek
 * @date 2026
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef PROTOBUF_KAFKA_KAFKAPRODUCER_HPP
#define PROTOBUF_KAFKA_KAFKAPRODUCER_HPP

#include "Config.hpp"

#include <memory>
#include <atomic>
#include <ctime>
#include <cstdint>
#include <pthread.h>

#include <librdkafka/rdkafka.h>
#include <ipfixcol2.h>

namespace protobuf_kafka {

/**
 * \brief Kafka producer
 */
class KafkaProducer {
public:
    /**
     * \brief Create and configure Kafka producer
     *
     * \param cfg  Plugin configuration
     * \param ctx  Plugin context for logging
     * \throws std::runtime_error on configuration or connection error
     */
    KafkaProducer(const Config& cfg, ipx_ctx_t* ctx);

    ~KafkaProducer();

    // Non-copyable
    KafkaProducer(const KafkaProducer&) = delete;
    KafkaProducer& operator=(const KafkaProducer&) = delete;

    /**
     * \brief Send a message to Kafka
     *
     * Uses RD_KAFKA_MSG_F_COPY so caller retains ownership of data.
     *
     * \param data       Pointer to message data
     * \param len        Length of message data
     * \param partition  Partition number or RD_KAFKA_PARTITION_UA for random
     * \return 0 on success, -1 on error (logged internally)
     */
    int produce(const char* data, size_t len, int32_t partition = RD_KAFKA_PARTITION_UA);

    /**
     * \brief Compute RSS partition from flow ID
     *
     * \param flow_id          Flow identifier
     * \param partition_count  Number of partitions
     * \return Partition number [0, partition_count)
     */
    static int32_t computeRssPartitionFromFlowId(
        uint64_t flow_id,
        int32_t partition_count);

    /**
     * \brief Compute RSS partition from 5-tuple
     *
     * Uses symmetric hashing so both directions of a flow go to the same partition.
     *
     * \param src_ip       Source IP address bytes
     * \param src_ip_len   Length of source IP (4 for IPv4, 16 for IPv6)
     * \param dst_ip       Destination IP address bytes
     * \param dst_ip_len   Length of destination IP
     * \param src_port     Source port (host byte order)
     * \param dst_port     Destination port (host byte order)
     * \param protocol     IP protocol number
     * \param partition_count  Number of partitions
     * \return Partition number [0, partition_count)
     */
    static int32_t computeRssPartition(
        const uint8_t* src_ip, size_t src_ip_len,
        const uint8_t* dst_ip, size_t dst_ip_len,
        uint16_t src_port, uint16_t dst_port,
        uint8_t protocol,
        int32_t partition_count);

    /**
     * \brief Get the number of partitions for the topic (queried once at startup)
     * \return Number of partitions, or 0 if unknown
     */
    int32_t partitionCount() const { return m_partition_count; }

private:
    using uniq_kafka = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;
    using uniq_topic = std::unique_ptr<rd_kafka_topic_t, decltype(&rd_kafka_topic_destroy)>;

    /// Polling thread context
    struct ThreadContext {
        ipx_ctx_t* ctx;              ///< Plugin context for logging
        pthread_t thread;            ///< Thread handle
        std::atomic<bool> stop;      ///< Stop flag
        rd_kafka_t* kafka;           ///< Kafka handle for polling

        std::atomic<uint64_t> cnt_delivered;  ///< Successful deliveries (thread-safe)
        std::atomic<uint64_t> cnt_failed;     ///< Failed deliveries (thread-safe)
    };

    static constexpr int POLLER_TIMEOUT = 100;   ///< Poll timeout in ms
    static constexpr int FLUSH_TIMEOUT = 3000;   ///< Flush timeout in ms

    ipx_ctx_t* m_ctx;
    uniq_kafka m_kafka = {nullptr, &rd_kafka_destroy};
    uniq_topic m_topic = {nullptr, &rd_kafka_topic_destroy};
    std::unique_ptr<ThreadContext> m_thread;
    int m_produce_flags;
    int32_t m_partition_count = 0;

    struct timespec m_err_ts;
    rd_kafka_resp_err_t m_err_type = RD_KAFKA_RESP_ERR_NO_ERROR;
    uint64_t m_err_cnt = 0;
    pthread_mutex_t m_err_lock = PTHREAD_MUTEX_INITIALIZER;

    /// Aggregate/log produce errors. Caller must hold m_err_lock.
    void produceError(struct timespec ts_now);

    static void* threadPolling(void* context);
    static void threadDeliveryCallback(rd_kafka_t* rk,
                                        const rd_kafka_message_t* rkmessage,
                                        void* opaque);

    void queryPartitionCount();
};

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_KAFKAPRODUCER_HPP
