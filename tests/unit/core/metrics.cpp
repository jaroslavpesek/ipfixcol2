#include <gtest/gtest.h>
#include <ipfixcol2.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string>

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(Metrics, RegistrationIsIdempotent)
{
    const struct ipx_metric_label lbl = {"a", "1"};
    ipx_metric_t *m1 = ipx_metric_new(IPX_METRIC_COUNTER, "test_idem_total", "help", &lbl, 1);
    ipx_metric_t *m2 = ipx_metric_new(IPX_METRIC_COUNTER, "test_idem_total", "help", &lbl, 1);
    ASSERT_NE(m1, nullptr);
    EXPECT_EQ(m1, m2);
    const struct ipx_metric_label other = {"a", "2"};
    EXPECT_NE(m1, ipx_metric_new(IPX_METRIC_COUNTER, "test_idem_total", "help", &other, 1));
}

TEST(Metrics, AddSetGet)
{
    ipx_metric_t *c = ipx_metric_new(IPX_METRIC_COUNTER, "test_add_total", "help", nullptr, 0);
    ipx_metric_add(c, 2);
    ipx_metric_add(c, 3);
    EXPECT_EQ(ipx_metric_get(c), 5U);
    ipx_metric_t *g = ipx_metric_new(IPX_METRIC_GAUGE, "test_gauge", "help", nullptr, 0);
    ipx_metric_set(g, 7);
    EXPECT_EQ(ipx_metric_get(g), 7U);
    // NULL is a no-op
    ipx_metric_add(nullptr, 1);
    ipx_metric_set(nullptr, 1);
    EXPECT_EQ(ipx_metric_get(nullptr), 0U);
}

TEST(Metrics, RenderAndEscape)
{
    const struct ipx_metric_label lbl = {"name", "a\"b\\c\nd"};
    ipx_metric_t *c = ipx_metric_new(IPX_METRIC_COUNTER, "test_render_total", "he\nlp", &lbl, 1);
    ipx_metric_add(c, 4);
    char *text = ipx_metrics_render();
    ASSERT_NE(text, nullptr);
    std::string s(text);
    free(text);
    EXPECT_NE(s.find("# HELP test_render_total he\\nlp\n"), std::string::npos);
    EXPECT_NE(s.find("# TYPE test_render_total counter\n"), std::string::npos);
    EXPECT_NE(s.find("test_render_total{name=\"a\\\"b\\\\c\\nd\"} 4\n"), std::string::npos);
    // The family header is rendered once even with several series
    const struct ipx_metric_label lbl2 = {"name", "x"};
    ipx_metric_new(IPX_METRIC_COUNTER, "test_render_total", "he\nlp", &lbl2, 1);
    text = ipx_metrics_render();
    s = text;
    free(text);
    EXPECT_EQ(s.find("# TYPE test_render_total counter"), s.rfind("# TYPE test_render_total counter"));
}

static std::string
http_get(uint16_t port, const char *path)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) != 0) {
        close(fd);
        return "";
    }
    std::string req = std::string("GET ") + path + " HTTP/1.1\r\nHost: x\r\n\r\n";
    (void) !write(fd, req.data(), req.size());
    std::string out;
    char buf[4096];
    ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        out.append(buf, n);
    }
    close(fd);
    return out;
}

TEST(Metrics, HttpEndpoint)
{
    ipx_metric_add(ipx_metric_new(IPX_METRIC_COUNTER, "test_http_total", "help", nullptr, 0), 1);
    ASSERT_EQ(ipx_metrics_start("127.0.0.1:0"), IPX_OK);
    uint16_t port = ipx_metrics_port();
    ASSERT_NE(port, 0);
    std::string ok = http_get(port, "/metrics");
    EXPECT_EQ(ok.compare(0, 15, "HTTP/1.1 200 OK"), 0);
    EXPECT_NE(ok.find("test_http_total 1\n"), std::string::npos);
    std::string nf = http_get(port, "/other");
    EXPECT_EQ(nf.compare(0, 12, "HTTP/1.1 404"), 0);
    ipx_metrics_stop();
    EXPECT_EQ(ipx_metrics_port(), 0);
    ipx_metrics_stop(); // safe when not running
}
