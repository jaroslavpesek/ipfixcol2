/**
 * \file include/ipfixcol2/metrics.h
 * \brief Process-wide metrics registry and Prometheus text endpoint (public API)
 *
 * Counters and gauges are 64-bit unsigned values updated with relaxed atomics.
 * Registration is idempotent: the same name and label set returns the same series.
 * Metrics live for the whole process and are never freed.
 */

#ifndef IPX_METRICS_H
#define IPX_METRICS_H

#include <stddef.h>
#include <stdint.h>
#include <ipfixcol2/api.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ipx_ctx;

/** Metric type (rendered as Prometheus "# TYPE")                                  */
enum ipx_metric_type {
    IPX_METRIC_COUNTER,   /**< Monotonic counter                                  */
    IPX_METRIC_GAUGE      /**< Value that can go up and down                      */
};

/** One label of a series                                                          */
struct ipx_metric_label {
    const char *key;
    const char *value;
};

/** Metric series handle                                                           */
typedef struct ipx_metric ipx_metric_t;

/**
 * \brief Register (or find) a metric series
 * \param[in] type   Metric type
 * \param[in] name   Metric name (e.g. "ipfixcol2_udp_datagrams_received_total")
 * \param[in] help   One-line description
 * \param[in] labels Labels (can be NULL)
 * \param[in] cnt    Number of labels
 * \return Series handle or NULL on a memory allocation failure
 */
IPX_API ipx_metric_t *
ipx_metric_new(enum ipx_metric_type type, const char *name, const char *help,
    const struct ipx_metric_label *labels, size_t cnt);

/**
 * \brief Register a metric series labeled with the plugin instance
 *
 * Adds the labels "ipx_instance" (the instance name from the configuration) and
 * "ipx_plugin" (the plugin name) in front of \p labels.
 * \copydetails ipx_metric_new()
 * \param[in] ctx Plugin context
 */
IPX_API ipx_metric_t *
ipx_ctx_metric_new(const struct ipx_ctx *ctx, enum ipx_metric_type type, const char *name,
    const char *help, const struct ipx_metric_label *labels, size_t cnt);

/** \brief Add a value to a series (no-op when \p metric is NULL)                  */
IPX_API void
ipx_metric_add(ipx_metric_t *metric, uint64_t value);

/** \brief Set the value of a series (no-op when \p metric is NULL)                */
IPX_API void
ipx_metric_set(ipx_metric_t *metric, uint64_t value);

/** \brief Get the value of a series (0 when \p metric is NULL)                    */
IPX_API uint64_t
ipx_metric_get(const ipx_metric_t *metric);

/**
 * \brief Render all series in the Prometheus text exposition format
 * \return Newly allocated string (the caller frees it) or NULL on failure
 */
IPX_API char *
ipx_metrics_render(void);

/**
 * \brief Start the HTTP endpoint serving "GET /metrics"
 * \param[in] listen Address to bind, "host:port" or "[v6]:port" (port 0 = any)
 * \return #IPX_OK on success, #IPX_ERR_DENIED otherwise (the reason is logged)
 */
IPX_API int
ipx_metrics_start(const char *listen);

/** \brief Port the endpoint is bound to (0 when not running)                      */
IPX_API uint16_t
ipx_metrics_port(void);

/** \brief Stop the HTTP endpoint (safe to call when not running)                  */
IPX_API void
ipx_metrics_stop(void);

#ifdef __cplusplus
}
#endif
#endif // IPX_METRICS_H
