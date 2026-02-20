/**
 * \file KafkaProducer.cpp
 * \brief Kafka producer wrapper
 * \author Jaroslav Pesek
 * \date 2026
 */

#include "KafkaProducer.hpp"

#include <cinttypes>
#include <cstring>
#include <stdexcept>
#include <xxhash.h>

namespace protobuf_kafka_compiled {

KafkaProducer::KafkaProducer(const Config &cfg, ipx_ctx_t *ctx) : m_ctx(ctx) {
  IPX_CTX_DEBUG(ctx, "Initializing Kafka producer...");
  IPX_CTX_INFO(ctx, "Using librdkafka version %s (built with %X)",
               rd_kafka_version_str(), RD_KAFKA_VERSION);

  char err_str[512];
  const size_t err_size = sizeof(err_str);

  clock_gettime(CLOCK_MONOTONIC, &m_err_ts);
  m_thread = std::make_unique<ThreadContext>();

  m_produce_flags = RD_KAFKA_MSG_F_COPY;
  if (cfg.blocking) {
    m_produce_flags |= RD_KAFKA_MSG_F_BLOCK;
  }

  rd_kafka_conf_t *kafka_cfg = rd_kafka_conf_new();
  if (!kafka_cfg) {
    throw std::runtime_error("rd_kafka_conf_new() failed");
  }

  auto set_config = [&](const char *key, const char *value) {
    IPX_CTX_DEBUG(ctx, "Setting Kafka config: %s=%s", key, value);
    rd_kafka_conf_res_t res =
        rd_kafka_conf_set(kafka_cfg, key, value, err_str, err_size);
    if (res != RD_KAFKA_CONF_OK) {
      rd_kafka_conf_destroy(kafka_cfg);
      throw std::runtime_error(std::string("Failed to set Kafka config '") +
                               key + "'='" + value + "': " + err_str);
    }
  };

  set_config("bootstrap.servers", cfg.brokers.c_str());
  set_config("batch.num.messages", std::to_string(cfg.batch_size).c_str());
  set_config("queue.buffering.max.ms", std::to_string(cfg.linger_ms).c_str());
  set_config("compression.codec", cfg.compression.c_str());
  rd_kafka_conf_set_dr_msg_cb(kafka_cfg, threadDeliveryCallback);
  rd_kafka_conf_set_opaque(kafka_cfg, m_thread.get());

  m_kafka.reset(rd_kafka_new(RD_KAFKA_PRODUCER, kafka_cfg, err_str, err_size));
  if (!m_kafka) {
    throw std::runtime_error(std::string("Failed to create Kafka producer: ") +
                             err_str);
  }

  m_topic.reset(rd_kafka_topic_new(m_kafka.get(), cfg.topic.c_str(), nullptr));
  if (!m_topic) {
    rd_kafka_resp_err_t err = rd_kafka_last_error();
    throw std::runtime_error(std::string("Failed to create Kafka topic: ") +
                             rd_kafka_err2str(err));
  }

  queryPartitionCount();

  m_thread->stop = false;
  m_thread->ctx = ctx;
  m_thread->kafka = m_kafka.get();
  m_thread->cnt_delivered = 0;
  m_thread->cnt_failed = 0;

  if (pthread_create(&m_thread->thread, nullptr, &threadPolling,
                     m_thread.get()) != 0) {
    throw std::runtime_error("Failed to start Kafka polling thread");
  }

  IPX_CTX_INFO(ctx, "Kafka producer initialized: topic=%s, partitions=%d",
               cfg.topic.c_str(), m_partition_count);
}

KafkaProducer::~KafkaProducer() {
  IPX_CTX_DEBUG(m_ctx, "Destroying Kafka producer...");

  if (m_thread) {
    m_thread->stop = true;
    pthread_join(m_thread->thread, nullptr);
  }

  if (m_kafka) {
    if (rd_kafka_flush(m_kafka.get(), FLUSH_TIMEOUT) ==
        RD_KAFKA_RESP_ERR__TIMED_OUT) {
      IPX_CTX_WARNING(m_ctx,
                      "Some Kafka messages were not delivered (timeout)");
    }
  }

  m_topic.reset();
  m_kafka.reset();

  IPX_CTX_DEBUG(m_ctx, "Kafka producer destroyed");
}

int KafkaProducer::produce(const char *data, size_t len, int32_t partition) {
  int rc = rd_kafka_produce(m_topic.get(), partition, m_produce_flags,
                            const_cast<char *>(data), len, // Payload
                            nullptr, 0,                    // Key
                            nullptr);                      // Opaque

  if (rc == 0 && m_err_cnt == 0) {
    return 0;
  }
  rd_kafka_resp_err_t err_code = rd_kafka_last_error();

  struct timespec ts_now;
  clock_gettime(CLOCK_MONOTONIC, &ts_now);

  if (rc != 0) {
    if (err_code != m_err_type) {
      produceError(ts_now);
      m_err_type = err_code;
    }
    m_err_cnt++;
  }

  if (difftime(ts_now.tv_sec, m_err_ts.tv_sec) >= 1.0) {
    produceError(ts_now);
  }

  return rc;
}

void KafkaProducer::produceError(struct timespec ts_now) {
  if (m_err_type == RD_KAFKA_RESP_ERR_NO_ERROR || m_err_cnt == 0) {
    return;
  }

  IPX_CTX_ERROR(m_ctx, "rd_kafka_produce() failed: %s (%" PRIu64 "x)",
                rd_kafka_err2str(m_err_type), m_err_cnt);

  m_err_ts = ts_now;
  m_err_type = RD_KAFKA_RESP_ERR_NO_ERROR;
  m_err_cnt = 0;
}

int32_t KafkaProducer::computeRssPartition(const uint8_t *src_ip,
                                           size_t src_ip_len,
                                           const uint8_t *dst_ip,
                                           size_t dst_ip_len, uint16_t src_port,
                                           uint16_t dst_port, uint8_t protocol,
                                           int32_t partition_count) {
  if (partition_count <= 0) {
    return RD_KAFKA_PARTITION_UA;
  }

  uint8_t key_buffer[64];
  size_t key_len = 0;

  size_t max_ip_len = (src_ip_len > dst_ip_len) ? src_ip_len : dst_ip_len;
  for (size_t i = 0; i < max_ip_len; ++i) {
    uint8_t src_byte = (i < src_ip_len && src_ip) ? src_ip[i] : 0;
    uint8_t dst_byte = (i < dst_ip_len && dst_ip) ? dst_ip[i] : 0;
    key_buffer[key_len++] = src_byte ^ dst_byte;
  }

  uint16_t port_xor = src_port ^ dst_port;
  key_buffer[key_len++] = static_cast<uint8_t>(port_xor >> 8);
  key_buffer[key_len++] = static_cast<uint8_t>(port_xor & 0xFF);
  key_buffer[key_len++] = protocol;

  uint64_t hash = XXH64(key_buffer, key_len, 0);

  return static_cast<int32_t>(hash % static_cast<uint64_t>(partition_count));
}

void *KafkaProducer::threadPolling(void *context) {
  auto *data = static_cast<ThreadContext *>(context);
  IPX_CTX_DEBUG(data->ctx, "Kafka polling thread started");

  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  while (!data->stop) {
    rd_kafka_poll(data->kafka, POLLER_TIMEOUT);

    struct timespec ts_now;
    clock_gettime(CLOCK_MONOTONIC, &ts_now);
    if (difftime(ts_now.tv_sec, ts.tv_sec) >= 1.0) {
      ts = ts_now;
      uint64_t delivered = data->cnt_delivered.exchange(0);
      uint64_t failed = data->cnt_failed.exchange(0);
      if (delivered > 0 || failed > 0) {
        IPX_CTX_DEBUG(data->ctx,
                      "Kafka stats: delivered=%" PRIu64 ", failed=%" PRIu64,
                      delivered, failed);
      }
    }
  }

  IPX_CTX_DEBUG(data->ctx, "Kafka polling thread stopped");
  return nullptr;
}

void KafkaProducer::threadDeliveryCallback(rd_kafka_t *rk,
                                           const rd_kafka_message_t *rkmessage,
                                           void *opaque) {
  (void)rk;
  auto *data = static_cast<ThreadContext *>(opaque);

  if (rkmessage->err) {
    data->cnt_failed.fetch_add(1, std::memory_order_relaxed);
  } else {
    data->cnt_delivered.fetch_add(1, std::memory_order_relaxed);
  }
}

void KafkaProducer::queryPartitionCount() {
  const struct rd_kafka_metadata *metadata = nullptr;
  rd_kafka_resp_err_t err =
      rd_kafka_metadata(m_kafka.get(), 0, m_topic.get(), &metadata, 5000);

  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    IPX_CTX_WARNING(m_ctx, "Failed to get topic metadata: %s",
                    rd_kafka_err2str(err));
    m_partition_count = 1;
    return;
  }

  if (metadata->topic_cnt > 0 && metadata->topics[0].partition_cnt > 0) {
    m_partition_count = metadata->topics[0].partition_cnt;
  } else {
    m_partition_count = 1;
  }

  rd_kafka_metadata_destroy(metadata);
}

} // namespace protobuf_kafka_compiled
