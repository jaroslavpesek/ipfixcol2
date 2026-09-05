/**
 * \file src/core/metrics.c
 * \brief Metrics registry and Prometheus text endpoint (source file)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <ipfixcol2.h>
#include "context.h"
#include "verbose.h"

/** Log module name */
#define MODULE "Metrics"

struct ipx_metric {
    struct ipx_metric *next;
    enum ipx_metric_type type;
    char *name;
    char *help;
    /** Rendered label set, e.g. a="1",b="2" (empty string when there are no labels) */
    char *labels;
    /** Accessed with __atomic builtins only */
    uint64_t value;
};

/** Series sorted by name so a family renders as one block */
static struct ipx_metric *registry = NULL;
static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;

// -------------------------------------------------------------------------------------------------
// Growable string

struct strbuf {
    char *data;
    size_t len;
    size_t cap;
};

static int
strbuf_printf(struct strbuf *b, const char *fmt, ...)
{
    for (;;) {
        va_list ap;
        va_start(ap, fmt);
        int n = vsnprintf(b->data + b->len, b->cap - b->len, fmt, ap);
        va_end(ap);
        if (n < 0) {
            return -1;
        }
        if ((size_t) n < b->cap - b->len) {
            b->len += (size_t) n;
            return 0;
        }

        size_t cap = b->cap ? b->cap * 2 : 4096;
        while (cap - b->len <= (size_t) n) {
            cap *= 2;
        }
        char *tmp = realloc(b->data, cap);
        if (!tmp) {
            return -1;
        }
        b->data = tmp;
        b->cap = cap;
    }
}

/** Append \p str escaped for the text format (\p quote also escapes double quotes) */
static int
strbuf_escape(struct strbuf *b, const char *str, bool quote)
{
    for (; *str; ++str) {
        const char *rep = NULL;
        if (*str == '\\') {
            rep = "\\\\";
        } else if (*str == '\n') {
            rep = "\\n";
        } else if (quote && *str == '"') {
            rep = "\\\"";
        }
        int rc = rep ? strbuf_printf(b, "%s", rep) : strbuf_printf(b, "%c", *str);
        if (rc) {
            return rc;
        }
    }
    return 0;
}

// -------------------------------------------------------------------------------------------------
// Registry

static char *
labels_render(const struct ipx_metric_label *labels, size_t cnt)
{
    struct strbuf b = {0};
    for (size_t i = 0; i < cnt; ++i) {
        if (strbuf_printf(&b, "%s%s=\"", i ? "," : "", labels[i].key)
                || strbuf_escape(&b, labels[i].value, true)
                || strbuf_printf(&b, "\"")) {
            free(b.data);
            return NULL;
        }
    }
    char *out = strdup(b.data ? b.data : "");
    free(b.data);
    return out;
}

ipx_metric_t *
ipx_metric_new(enum ipx_metric_type type, const char *name, const char *help,
    const struct ipx_metric_label *labels, size_t cnt)
{
    char *lbl = labels_render(labels, cnt);
    if (!lbl) {
        return NULL;
    }

    pthread_mutex_lock(&registry_lock);
    struct ipx_metric **pos = &registry;
    for (; *pos; pos = &(*pos)->next) {
        int cmp = strcmp((*pos)->name, name);
        if (cmp > 0) {
            break;
        }
        if (cmp == 0 && strcmp((*pos)->labels, lbl) == 0) {
            pthread_mutex_unlock(&registry_lock);
            free(lbl);
            return *pos;
        }
    }

    struct ipx_metric *m = calloc(1, sizeof(*m));
    if (m) {
        m->type = type;
        m->name = strdup(name);
        m->help = strdup(help);
        m->labels = lbl;
    }
    if (!m || !m->name || !m->help) {
        pthread_mutex_unlock(&registry_lock);
        if (m) {
            free(m->name);
            free(m->help);
            free(m);
        }
        free(lbl);
        return NULL;
    }

    m->next = *pos;
    *pos = m;
    pthread_mutex_unlock(&registry_lock);
    return m;
}

ipx_metric_t *
ipx_ctx_metric_new(const struct ipx_ctx *ctx, enum ipx_metric_type type, const char *name,
    const char *help, const struct ipx_metric_label *labels, size_t cnt)
{
    const struct ipx_plugin_info *info = ipx_ctx_plugininfo_get(ctx);
    struct ipx_metric_label all[cnt + 2];
    all[0] = (struct ipx_metric_label) {"ipx_instance", ipx_ctx_name_get(ctx)};
    all[1] = (struct ipx_metric_label) {"ipx_plugin", info ? info->name : ""};
    for (size_t i = 0; i < cnt; ++i) {
        all[i + 2] = labels[i];
    }
    return ipx_metric_new(type, name, help, all, cnt + 2);
}

void
ipx_metric_add(ipx_metric_t *metric, uint64_t value)
{
    if (metric) {
        __atomic_fetch_add(&metric->value, value, __ATOMIC_RELAXED);
    }
}

void
ipx_metric_set(ipx_metric_t *metric, uint64_t value)
{
    if (metric) {
        __atomic_store_n(&metric->value, value, __ATOMIC_RELAXED);
    }
}

uint64_t
ipx_metric_get(const ipx_metric_t *metric)
{
    return metric ? __atomic_load_n(&metric->value, __ATOMIC_RELAXED) : 0;
}

char *
ipx_metrics_render(void)
{
    static const char *type_str[] = {"counter", "gauge"};
    struct strbuf b = {0};
    const char *family = "";
    bool failed = false;

    pthread_mutex_lock(&registry_lock);
    for (const struct ipx_metric *m = registry; m && !failed; m = m->next) {
        int rc = 0;
        if (strcmp(family, m->name) != 0) {
            family = m->name;
            rc = strbuf_printf(&b, "# HELP %s ", m->name)
                || strbuf_escape(&b, m->help, false)
                || strbuf_printf(&b, "\n# TYPE %s %s\n", m->name, type_str[m->type]);
        }
        if (m->labels[0]) {
            rc = rc || strbuf_printf(&b, "%s{%s} %" PRIu64 "\n", m->name, m->labels,
                ipx_metric_get(m));
        } else {
            rc = rc || strbuf_printf(&b, "%s %" PRIu64 "\n", m->name, ipx_metric_get(m));
        }
        failed = rc != 0;
    }
    pthread_mutex_unlock(&registry_lock);

    if (failed) {
        free(b.data);
        return NULL;
    }
    return b.data ? b.data : strdup("");
}

// -------------------------------------------------------------------------------------------------
// HTTP endpoint

static struct {
    int fd;
    uint16_t port;
    pthread_t thread;
    bool running;
} server = {.fd = -1};

/** Answer one request: "GET /metrics" gets the registry, anything else 404 */
static void
client_serve(int fd)
{
    struct timeval tv = {.tv_sec = 2};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    char req[4096];
    size_t len = 0;
    while (len < sizeof(req) - 1) {
        ssize_t n = recv(fd, req + len, sizeof(req) - 1 - len, 0);
        if (n <= 0) {
            break;
        }
        len += (size_t) n;
        req[len] = '\0';
        if (strstr(req, "\r\n\r\n") || strstr(req, "\n\n")) {
            break;
        }
    }
    req[len] = '\0';

    char *body = NULL;
    const char *status = "404 Not Found";
    if (strncmp(req, "GET /metrics", 12) == 0 && (req[12] == ' ' || req[12] == '?')) {
        body = ipx_metrics_render();
        status = body ? "200 OK" : "500 Internal Server Error";
    }

    struct strbuf out = {0};
    if (strbuf_printf(&out, "HTTP/1.1 %s\r\n"
            "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
            "Content-Length: %zu\r\nConnection: close\r\n\r\n%s",
            status, body ? strlen(body) : 0, body ? body : "") == 0) {
        for (size_t sent = 0; sent < out.len;) {
            ssize_t n = send(fd, out.data + sent, out.len - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                break;
            }
            sent += (size_t) n;
        }
    }
    free(out.data);
    free(body);
}

static void *
server_thread(void *arg)
{
    (void) arg;
    // Signals are handled by the main thread
    sigset_t set;
    sigfillset(&set);
    pthread_sigmask(SIG_SETMASK, &set, NULL);
    pthread_setname_np(pthread_self(), "ipx:metrics");

    for (;;) {
        int fd = accept(server.fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        client_serve(fd);
        close(fd);
    }
    return NULL;
}

int
ipx_metrics_start(const char *listen_addr)
{
    if (server.running) {
        return IPX_OK;
    }

    // Split "host:port" or "[v6]:port" at the last colon
    const char *colon = strrchr(listen_addr, ':');
    const char *host_start = listen_addr;
    size_t host_len = colon ? (size_t) (colon - listen_addr) : 0;
    if (host_len >= 2 && host_start[0] == '[' && host_start[host_len - 1] == ']') {
        host_start++;
        host_len -= 2;
    }
    char host[256];
    if (!colon || host_len == 0 || host_len >= sizeof(host)) {
        IPX_ERROR(MODULE, "Invalid listen address '%s' (expected host:port)", listen_addr);
        return IPX_ERR_DENIED;
    }
    memcpy(host, host_start, host_len);
    host[host_len] = '\0';

    struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM,
        .ai_flags = AI_PASSIVE};
    struct addrinfo *res;
    int rc = getaddrinfo(host, colon + 1, &hints, &res);
    if (rc != 0) {
        IPX_ERROR(MODULE, "Unable to resolve listen address '%s': %s", listen_addr,
            gai_strerror(rc));
        return IPX_ERR_DENIED;
    }

    int fd = -1;
    int err = 0;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            err = errno;
            continue;
        }
        int on = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        if (bind(fd, ai->ai_addr, ai->ai_addrlen) == 0 && listen(fd, 16) == 0) {
            break;
        }
        err = errno;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) {
        const char *err_str;
        ipx_strerror(err, err_str);
        IPX_ERROR(MODULE, "Unable to listen on '%s': %s", listen_addr, err_str);
        return IPX_ERR_DENIED;
    }

    struct sockaddr_storage ss;
    socklen_t ss_len = sizeof(ss);
    getsockname(fd, (struct sockaddr *) &ss, &ss_len);
    server.port = ntohs(ss.ss_family == AF_INET6
        ? ((struct sockaddr_in6 *) &ss)->sin6_port
        : ((struct sockaddr_in *) &ss)->sin_port);
    server.fd = fd;

    if (pthread_create(&server.thread, NULL, server_thread, NULL) != 0) {
        IPX_ERROR(MODULE, "Unable to start the metrics thread", '\0');
        close(fd);
        server.fd = -1;
        server.port = 0;
        return IPX_ERR_DENIED;
    }
    server.running = true;
    IPX_INFO(MODULE, "Serving metrics on '%s' (port %" PRIu16 ")", listen_addr, server.port);
    return IPX_OK;
}

uint16_t
ipx_metrics_port(void)
{
    return server.port;
}

void
ipx_metrics_stop(void)
{
    if (!server.running) {
        return;
    }
    // ponytail: Linux-only; shutdown() on the listening socket wakes the blocked accept()
    shutdown(server.fd, SHUT_RDWR);
    pthread_join(server.thread, NULL);
    close(server.fd);
    server.fd = -1;
    server.port = 0;
    server.running = false;
}
