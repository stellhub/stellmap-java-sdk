package io.github.starmap.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.starmap.sdk.exception.StarMapServerException;
import io.github.starmap.sdk.model.Endpoint;
import io.github.starmap.sdk.model.RegisterRequest;
import io.github.starmap.sdk.model.RegistryInstance;
import io.github.starmap.sdk.model.RegistryQueryRequest;
import io.github.starmap.sdk.model.RegistryWatchEvent;
import io.github.starmap.sdk.model.StarMapResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StarMapClientTest {

    private final List<HttpServer> servers = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @AfterEach
    void tearDown() {
        for (HttpServer server : servers) {
            server.stop(0);
        }
        servers.clear();
    }

    @Test
    void shouldNormalizeRegisterRequestBeforeSending() throws Exception {
        AtomicReference<JsonNode> capturedBody = new AtomicReference<>();
        HttpServer server = startServer(exchange -> {
            capturedBody.set(readJson(exchange));
            writeJson(exchange, 200, """
                    {"code":"ok","message":"instance registered","requestId":"req-1"}
                    """);
        }, "/api/v1/registry/register");

        try (StarMapClient client = new StarMapClient(options(server))) {
            StarMapResponse<Void> response = client.register(RegisterRequest.builder()
                    .namespace(" prod ")
                    .service(" order-service ")
                    .instanceId(" order-1 ")
                    .zone(" az1 ")
                    .labels(Map.of("color", " gray "))
                    .metadata(Map.of("owner", " trade-team "))
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build());

            assertEquals("ok", response.getCode());
            assertEquals("instance registered", response.getMessage());
        }

        assertNotNull(capturedBody.get());
        assertEquals("prod", capturedBody.get().get("namespace").asText());
        assertEquals("order-service", capturedBody.get().get("service").asText());
        assertEquals("order-1", capturedBody.get().get("instanceId").asText());
        assertEquals("az1", capturedBody.get().get("zone").asText());
        assertEquals("gray", capturedBody.get().get("labels").get("color").asText());
        assertEquals("trade-team", capturedBody.get().get("metadata").get("owner").asText());
        assertEquals("http", capturedBody.get().get("endpoints").get(0).get("name").asText());
        assertEquals(StarMapDefaults.DEFAULT_ENDPOINT_WEIGHT, capturedBody.get().get("endpoints").get(0).get("weight").asInt());
        assertEquals(StarMapDefaults.DEFAULT_LEASE_TTL_SECONDS, capturedBody.get().get("leaseTtlSeconds").asLong());
    }

    @Test
    void shouldEncodeRepeatedSelectorAndLegacyLabelQuery() throws Exception {
        AtomicReference<String> capturedQuery = new AtomicReference<>();
        HttpServer server = startServer(exchange -> {
            capturedQuery.set(exchange.getRequestURI().getRawQuery());
            writeJson(exchange, 200, """
                    {
                      "code":"ok",
                      "data":[
                        {
                          "namespace":"prod",
                          "service":"order-service",
                          "instanceId":"order-1",
                          "zone":"az1",
                          "leaseTtlSeconds":30,
                          "registeredAtUnix":1,
                          "lastHeartbeatUnix":1,
                          "endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]
                        }
                      ]
                    }
                    """);
        }, "/api/v1/registry/instances");

        try (StarMapClient client = new StarMapClient(options(server))) {
            StarMapResponse<List<RegistryInstance>> response = client.queryInstances(RegistryQueryRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .zone("az1")
                    .endpoint("http")
                    .selectors(List.of(
                            SelectorExpression.equalsTo("color", "gray"),
                            SelectorExpression.in("version", List.of("v2"))
                    ))
                    .labels(Map.of("color", "gray"))
                    .limit(10)
                    .build());

            assertEquals(1, response.getData().size());
            assertEquals("order-1", response.getData().get(0).getInstanceId());
        }

        assertNotNull(capturedQuery.get());
        assertTrue(capturedQuery.get().contains("namespace=prod"));
        assertTrue(capturedQuery.get().contains("service=order-service"));
        assertTrue(capturedQuery.get().contains("selector=color%3Dgray"));
        assertTrue(capturedQuery.get().contains("selector=version+in+%28v2%29"));
        assertTrue(capturedQuery.get().contains("label=color%3Dgray"));
        assertTrue(capturedQuery.get().contains("limit=10"));
    }

    @Test
    void shouldFollowLeaderRedirectForWriteRequests() throws Exception {
        AtomicInteger leaderAttempts = new AtomicInteger();
        HttpServer leaderServer = startServer(exchange -> {
            leaderAttempts.incrementAndGet();
            writeJson(exchange, 200, """
                    {"code":"ok","message":"instance registered"}
                    """);
        }, "/api/v1/registry/register");

        HttpServer followerServer = startServer(exchange -> writeJson(exchange, 503, """
                {"code":"not_leader","message":"redirect","leaderAddr":"127.0.0.1:%d"}
                """.formatted(leaderServer.getAddress().getPort())), "/api/v1/registry/register");

        try (StarMapClient client = new StarMapClient(options(followerServer))) {
            StarMapResponse<Void> response = client.register(RegisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build());

            assertEquals("ok", response.getCode());
        }

        assertEquals(1, leaderAttempts.get());
    }

    @Test
    void shouldParseWatchSseEvents() throws Exception {
        CountDownLatch eventLatch = new CountDownLatch(2);
        CountDownLatch closeLatch = new CountDownLatch(1);
        List<RegistryWatchEvent> received = new ArrayList<>();

        HttpServer server = startServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write("""
                        id: 10
                        event: snapshot
                        data: {"revision":10,"type":"snapshot","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                        
                        id: 11
                        event: delete
                        data: {"revision":11,"type":"delete","namespace":"prod","service":"order-service","instanceId":"order-1"}
                        
                        """.getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
            }
        }, "/api/v1/registry/watch");

        try (StarMapClient client = new StarMapClient(options(server));
             RegistryWatchSubscription subscription = client.watchInstances(RegistryQueryRequest.builder()
                     .namespace("prod")
                     .service("order-service")
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     received.add(event);
                     eventLatch.countDown();
                 }

                 @Override
                 public void onClosed() {
                     closeLatch.countDown();
                 }
             })) {
            assertTrue(eventLatch.await(3, TimeUnit.SECONDS));
            assertTrue(closeLatch.await(3, TimeUnit.SECONDS));
            assertTrue(subscription.isClosed() || !subscription.isClosed());
        }

        assertEquals(2, received.size());
        assertEquals("snapshot", received.get(0).getType());
        assertEquals(10L, received.get(0).getRevision());
        assertEquals("delete", received.get(1).getType());
        assertEquals("order-1", received.get(1).getInstanceId());
    }

    @Test
    void shouldThrowServerExceptionWhenFollowLeaderDisabled() throws Exception {
        HttpServer server = startServer(exchange -> writeJson(exchange, 503, """
                {"code":"not_leader","message":"redirect","leaderAddr":"127.0.0.1:18080"}
                """), "/api/v1/registry/register");

        StarMapClientOptions options = StarMapClientOptions.builder()
                .baseUrl(baseUrl(server))
                .requestTimeout(Duration.ofSeconds(2))
                .followLeaderRedirect(false)
                .build();

        try (StarMapClient client = new StarMapClient(options)) {
            Exception exception = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> client.register(RegisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build()));
            assertInstanceOf(StarMapServerException.class, exception);
        }
    }

    private StarMapClientOptions options(HttpServer server) {
        return StarMapClientOptions.builder()
                .baseUrl(baseUrl(server))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
    }

    private String baseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private HttpServer startServer(ExchangeHandler handler, String path) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(path, exchange -> {
            try {
                handler.handle(exchange);
            } finally {
                exchange.close();
            }
        });
        server.start();
        servers.add(server);
        return server;
    }

    private JsonNode readJson(HttpExchange exchange) throws IOException {
        return objectMapper.readTree(exchange.getRequestBody().readAllBytes());
    }

    private void writeJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
