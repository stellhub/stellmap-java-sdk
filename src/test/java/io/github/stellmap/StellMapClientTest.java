package io.github.stellmap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.stellmap.exception.StellMapServerException;
import io.github.stellmap.model.DeregisterRequest;
import io.github.stellmap.model.Endpoint;
import io.github.stellmap.model.HeartbeatRequest;
import io.github.stellmap.model.RegisterRequest;
import io.github.stellmap.model.RegistryInstance;
import io.github.stellmap.model.RegistryQueryRequest;
import io.github.stellmap.model.RegistryWatchEvent;
import io.github.stellmap.model.RegistryWatchRequest;
import io.github.stellmap.model.StarMapResponse;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("The execution is too slow; it's unnecessary.")
class StellMapClientTest {

    private final List<HttpServer> servers = new ArrayList<>();
    private final List<ExecutorService> executors = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @AfterEach
    void tearDown() {
        for (HttpServer server : servers) {
            server.stop(0);
        }
        servers.clear();
        for (ExecutorService executor : executors) {
            executor.shutdownNow();
        }
        executors.clear();
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

        try (StellMapClient client = new StellMapClient(options(server))) {
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
        assertEquals(StellMapDefaults.DEFAULT_ENDPOINT_WEIGHT, capturedBody.get().get("endpoints").get(0).get("weight").asInt());
        assertEquals(StellMapDefaults.DEFAULT_LEASE_TTL_SECONDS, capturedBody.get().get("leaseTtlSeconds").asLong());
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

        try (StellMapClient client = new StellMapClient(options(server))) {
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
            assertEquals("order-1", response.getData().getFirst().getInstanceId());
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
    void shouldComposeStructuredServiceIdentityAcrossRequests() throws Exception {
        AtomicReference<JsonNode> capturedRegisterBody = new AtomicReference<>();
        AtomicReference<String> capturedQuery = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        executors.add(executor);
        server.createContext("/api/v1/registry/register", exchange -> {
            try {
                capturedRegisterBody.set(readJson(exchange));
                writeJson(exchange, 200, """
                        {"code":"ok","message":"instance registered"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/api/v1/registry/instances", exchange -> {
            try {
                capturedQuery.set(exchange.getRequestURI().getRawQuery());
                writeJson(exchange, 200, """
                        {"code":"ok","data":[]}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.start();
        servers.add(server);

        try (StellMapClient client = new StellMapClient(options(server))) {
            client.register(RegisterRequest.builder()
                    .namespace("prod")
                    .organization("company")
                    .businessDomain("trade")
                    .capabilityDomain("order")
                    .application("order-center")
                    .role("api")
                    .instanceId("order-1")
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build());

            client.queryInstances(RegistryQueryRequest.builder()
                    .namespace("prod")
                    .organization("company")
                    .businessDomain("trade")
                    .capabilityDomain("order")
                    .build());
        }

        assertNotNull(capturedRegisterBody.get());
        assertEquals("company.trade.order.order-center.api", capturedRegisterBody.get().get("service").asText());
        assertEquals("company", capturedRegisterBody.get().get("organization").asText());
        assertEquals("trade", capturedRegisterBody.get().get("businessDomain").asText());
        assertEquals("order", capturedRegisterBody.get().get("capabilityDomain").asText());
        assertEquals("order-center", capturedRegisterBody.get().get("application").asText());
        assertEquals("api", capturedRegisterBody.get().get("role").asText());

        assertNotNull(capturedQuery.get());
        assertTrue(capturedQuery.get().contains("servicePrefix=company.trade.order"));
        assertTrue(capturedQuery.get().contains("organization=company"));
        assertTrue(capturedQuery.get().contains("businessDomain=trade"));
        assertTrue(capturedQuery.get().contains("capabilityDomain=order"));
    }

    @Test
    void shouldFollowLeaderRedirectForWriteRequests() throws Exception {
        AtomicInteger followerAttempts = new AtomicInteger();
        AtomicInteger leaderAttempts = new AtomicInteger();
        HttpServer leaderServer = startServer(exchange -> {
            leaderAttempts.incrementAndGet();
            writeJson(exchange, 200, """
                    {"code":"ok","message":"instance registered"}
                    """);
        }, "/api/v1/registry/register");

        HttpServer followerServer = startServer(exchange -> {
            followerAttempts.incrementAndGet();
            writeJson(exchange, 503, """
                    {"code":"not_leader","message":"redirect","leaderId":2,"leaderAddr":"127.0.0.1:%d"}
                    """.formatted(leaderServer.getAddress().getPort()));
        }, "/api/v1/registry/register");

        try (StellMapClient client = new StellMapClient(options(followerServer))) {
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

        assertEquals(1, followerAttempts.get());
        assertEquals(1, leaderAttempts.get());
    }

    @Test
    void shouldFollowLeaderRedirectForHeartbeatAndDeregisterRequests() throws Exception {
        AtomicInteger followerHeartbeatAttempts = new AtomicInteger();
        AtomicInteger followerDeregisterAttempts = new AtomicInteger();
        AtomicInteger leaderHeartbeatAttempts = new AtomicInteger();
        AtomicInteger leaderDeregisterAttempts = new AtomicInteger();

        HttpServer leaderServer = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService leaderExecutor = Executors.newCachedThreadPool();
        leaderServer.setExecutor(leaderExecutor);
        executors.add(leaderExecutor);
        leaderServer.createContext("/api/v1/registry/heartbeat", exchange -> {
            try {
                leaderHeartbeatAttempts.incrementAndGet();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"heartbeat accepted"}
                        """);
            } finally {
                exchange.close();
            }
        });
        leaderServer.createContext("/api/v1/registry/deregister", exchange -> {
            try {
                leaderDeregisterAttempts.incrementAndGet();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"instance deregistered"}
                        """);
            } finally {
                exchange.close();
            }
        });
        leaderServer.start();
        servers.add(leaderServer);

        HttpServer followerServer = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService followerExecutor = Executors.newCachedThreadPool();
        followerServer.setExecutor(followerExecutor);
        executors.add(followerExecutor);
        followerServer.createContext("/api/v1/registry/heartbeat", exchange -> {
            try {
                followerHeartbeatAttempts.incrementAndGet();
                writeJson(exchange, 503, """
                        {"code":"not_leader","message":"redirect","leaderId":2,"leaderAddr":"127.0.0.1:%d"}
                        """.formatted(leaderServer.getAddress().getPort()));
            } finally {
                exchange.close();
            }
        });
        followerServer.createContext("/api/v1/registry/deregister", exchange -> {
            try {
                followerDeregisterAttempts.incrementAndGet();
                writeJson(exchange, 503, """
                        {"code":"not_leader","message":"redirect","leaderId":2,"leaderAddr":"127.0.0.1:%d"}
                        """.formatted(leaderServer.getAddress().getPort()));
            } finally {
                exchange.close();
            }
        });
        followerServer.start();
        servers.add(followerServer);

        try (StellMapClient client = new StellMapClient(options(followerServer))) {
            StarMapResponse<Void> heartbeatResponse = client.heartbeat(HeartbeatRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .leaseTtlSeconds(30)
                    .build());
            assertEquals("ok", heartbeatResponse.getCode());

            StarMapResponse<Void> deregisterResponse = client.deregister(DeregisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .build());
            assertEquals("ok", deregisterResponse.getCode());
        }

        assertEquals(1, followerHeartbeatAttempts.get());
        assertEquals(1, leaderHeartbeatAttempts.get());
        assertEquals(1, followerDeregisterAttempts.get());
        assertEquals(1, leaderDeregisterAttempts.get());
    }

    @Test
    void shouldParseWatchSseEventsWhenAutoReconnectDisabled() throws Exception {
        CountDownLatch eventLatch = new CountDownLatch(2);
        CountDownLatch closeLatch = new CountDownLatch(1);
        List<RegistryWatchEvent> received = new CopyOnWriteArrayList<>();

        HttpServer server = startServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                writeSse(outputStream, """
                        id: 10
                        event: snapshot
                        data: {"revision":10,"type":"snapshot","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                        
                        id: 11
                        event: delete
                        data: {"revision":11,"type":"delete","namespace":"prod","service":"order-service","instanceId":"order-1"}
                        
                        """);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchAutoReconnect(false)
                .build();

        try (StellMapClient client = new StellMapClient(options);
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
            assertTrue(subscription.isClosed());
        }

        assertEquals(2, received.size());
        assertEquals("snapshot", received.get(0).getType());
        assertEquals(10L, received.get(0).getRevision());
        assertEquals("delete", received.get(1).getType());
        assertEquals("order-1", received.get(1).getInstanceId());
    }

    @Test
    void shouldAutoReconnectWatchAfterDisconnect() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch eventLatch = new CountDownLatch(2);
        List<RegistryWatchEvent> received = new CopyOnWriteArrayList<>();
        AtomicReference<RegistryWatchSubscription> subscriptionRef = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            int attempt = attempts.incrementAndGet();
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                if (attempt == 1) {
                    writeSse(outputStream, """
                            id: 1
                            event: snapshot
                            data: {"revision":1,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                            
                            """);
                    return;
                }
                writeSse(outputStream, """
                        id: 2
                        event: upsert
                        data: {"revision":2,"type":"UPSERT","namespace":"prod","service":"order-service","instance":{"namespace":"prod","service":"order-service","instanceId":"order-2","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8081,"weight":100}]}}
                        
                        """);
                awaitSilently(200);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchReconnectInitialDelay(Duration.ofMillis(100))
                .watchReconnectMaxDelay(Duration.ofMillis(100))
                .build();

        try (StellMapClient client = new StellMapClient(options);
             RegistryWatchSubscription subscription = client.watchInstances(RegistryQueryRequest.builder()
                     .namespace("prod")
                     .service("order-service")
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     received.add(event);
                     eventLatch.countDown();
                     if (event.getRevision() == 2L && subscriptionRef.get() != null) {
                         subscriptionRef.get().close();
                     }
                 }
             })) {
            subscriptionRef.set(subscription);
            assertTrue(eventLatch.await(5, TimeUnit.SECONDS));
        }

        assertTrue(attempts.get() >= 2);
        assertEquals(List.of(1L, 2L), received.stream().map(RegistryWatchEvent::getRevision).toList());
    }

    @Test
    void shouldUseSinceRevisionQueryParameterOnWatchResume() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<String> secondQuery = new AtomicReference<>();
        AtomicReference<String> secondLastEventId = new AtomicReference<>();
        CountDownLatch eventLatch = new CountDownLatch(2);
        AtomicReference<RegistryWatchSubscription> subscriptionRef = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            int attempt = attempts.incrementAndGet();
            if (attempt == 2) {
                secondQuery.set(exchange.getRequestURI().getRawQuery());
                secondLastEventId.set(exchange.getRequestHeaders().getFirst("Last-Event-ID"));
            }
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                if (attempt == 1) {
                    writeSse(outputStream, """
                            id: 10
                            event: snapshot
                            data: {"revision":10,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                            
                            """);
                    return;
                }
                writeSse(outputStream, """
                        id: 11
                        event: upsert
                        data: {"revision":11,"type":"UPSERT","namespace":"prod","service":"order-service","instance":{"namespace":"prod","service":"order-service","instanceId":"order-2","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8081,"weight":100}]}}
                        
                        """);
                awaitSilently(200);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchReconnectInitialDelay(Duration.ofMillis(100))
                .watchReconnectMaxDelay(Duration.ofMillis(100))
                .build();

        try (StellMapClient client = new StellMapClient(options);
             RegistryWatchSubscription subscription = client.watchInstances(RegistryQueryRequest.builder()
                     .namespace("prod")
                     .service("order-service")
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     eventLatch.countDown();
                     if (event.getRevision() == 11L && subscriptionRef.get() != null) {
                         subscriptionRef.get().close();
                     }
                 }
             })) {
            subscriptionRef.set(subscription);
            assertTrue(eventLatch.await(5, TimeUnit.SECONDS));
        }

        assertTrue(attempts.get() >= 2);
        assertNotNull(secondQuery.get());
        assertTrue(secondQuery.get().contains("sinceRevision=10"));
        assertTrue(secondQuery.get().contains("includeSnapshot=false"));
        assertNull(secondLastEventId.get());
    }

    @Test
    void shouldRebuildDirectoryWhenResetEventReceived() throws Exception {
        CountDownLatch eventLatch = new CountDownLatch(3);
        AtomicReference<ServiceDirectorySubscription> subscriptionRef = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                writeSse(outputStream, """
                        id: 1
                        event: snapshot
                        data: {"revision":1,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                        
                        id: 2
                        event: reset
                        data: {"revision":2,"type":"RESET"}
                        
                        id: 3
                        event: snapshot
                        data: {"revision":3,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-2","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8081,"weight":100}]}]}
                        
                        """);
                awaitSilently(200);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchAutoReconnect(false)
                .build();

        try (StellMapClient client = new StellMapClient(options);
             ServiceDirectorySubscription subscription = client.watchDirectory(RegistryWatchRequest.builder()
                     .namespace("prod")
                     .services(List.of("order-service"))
                     .includeSnapshot(true)
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     eventLatch.countDown();
                     if (event.getRevision() == 3L && subscriptionRef.get() != null) {
                         subscriptionRef.get().close();
                     }
                 }
             })) {
            subscriptionRef.set(subscription);
            assertTrue(eventLatch.await(5, TimeUnit.SECONDS));
            List<RegistryInstance> instances = subscription.getServiceDirectory().listInstances("prod", "order-service");
            assertEquals(1, instances.size());
            assertEquals("order-2", instances.getFirst().getInstanceId());
            assertEquals(3L, subscription.getLastRevision());
        }
    }

    @Test
    void shouldResetRevisionAndRebuildDirectoryWhenRevisionExpired() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<String> secondQuery = new AtomicReference<>();
        AtomicReference<String> thirdQuery = new AtomicReference<>();
        CountDownLatch finalSnapshotLatch = new CountDownLatch(1);
        AtomicReference<ServiceDirectorySubscription> subscriptionRef = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            int attempt = attempts.incrementAndGet();
            if (attempt == 2) {
                secondQuery.set(exchange.getRequestURI().getRawQuery());
                writeJson(exchange, 410, """
                        {"code":"revision_expired","message":"revision too old"}
                        """);
                return;
            }
            if (attempt == 3) {
                thirdQuery.set(exchange.getRequestURI().getRawQuery());
            }
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                if (attempt == 1) {
                    writeSse(outputStream, """
                            id: 10
                            event: snapshot
                            data: {"revision":10,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                            
                            """);
                    return;
                }
                writeSse(outputStream, """
                        id: 20
                        event: snapshot
                        data: {"revision":20,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-2","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8081,"weight":100}]}]}
                        
                        """);
                awaitSilently(200);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchReconnectInitialDelay(Duration.ofMillis(100))
                .watchReconnectMaxDelay(Duration.ofMillis(100))
                .build();

        try (StellMapClient client = new StellMapClient(options);
             ServiceDirectorySubscription subscription = client.watchDirectory(RegistryWatchRequest.builder()
                     .namespace("prod")
                     .services(List.of("order-service"))
                     .includeSnapshot(true)
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     if (event.getRevision() == 20L) {
                         finalSnapshotLatch.countDown();
                         if (subscriptionRef.get() != null) {
                             subscriptionRef.get().close();
                         }
                     }
                 }
             })) {
            subscriptionRef.set(subscription);
            assertTrue(finalSnapshotLatch.await(5, TimeUnit.SECONDS));
            List<RegistryInstance> instances = subscription.getServiceDirectory().listInstances("prod", "order-service");
            assertEquals(1, instances.size());
            assertEquals("order-2", instances.getFirst().getInstanceId());
            assertEquals(20L, subscription.getLastRevision());
        }

        assertTrue(attempts.get() >= 3);
        assertNotNull(secondQuery.get());
        assertTrue(secondQuery.get().contains("sinceRevision=10"));
        assertNotNull(thirdQuery.get());
        assertFalse(thirdQuery.get().contains("sinceRevision=10"));
        assertFalse(thirdQuery.get().contains("sinceRevision="));
    }

    @Test
    void shouldApplyAggregatedDirectoryEventsToLocalCache() throws Exception {
        CountDownLatch eventLatch = new CountDownLatch(3);
        AtomicReference<ServiceDirectorySubscription> subscriptionRef = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                writeSse(outputStream, """
                        id: 10
                        event: snapshot
                        data: {"revision":10,"type":"SNAPSHOT","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]},{"namespace":"prod","service":"payment-service","instanceId":"payment-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":9090,"weight":100}]}]}
                        
                        id: 11
                        event: upsert
                        data: {"revision":11,"type":"UPSERT","namespace":"prod","service":"order-service","instance":{"namespace":"prod","service":"order-service","instanceId":"order-2","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8081,"weight":100}]}}
                        
                        id: 12
                        event: delete
                        data: {"revision":12,"type":"DELETE","namespace":"prod","service":"payment-service","instanceId":"payment-1"}
                        
                        """);
                awaitSilently(200);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchAutoReconnect(false)
                .build();

        try (StellMapClient client = new StellMapClient(options);
             ServiceDirectorySubscription subscription = client.watchDirectory(RegistryWatchRequest.builder()
                     .namespace("prod")
                     .servicePrefixes(List.of("order-", "payment-"))
                     .includeSnapshot(true)
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     eventLatch.countDown();
                     if (event.getRevision() == 12L && subscriptionRef.get() != null) {
                         subscriptionRef.get().close();
                     }
                 }
             })) {
            subscriptionRef.set(subscription);
            assertTrue(eventLatch.await(5, TimeUnit.SECONDS));

            ServiceDirectory directory = subscription.getServiceDirectory();
            List<RegistryInstance> orderInstances = directory.listInstances("prod", "order-service");
            assertEquals(2, orderInstances.size());
            assertEquals(List.of("order-1", "order-2"), orderInstances.stream().map(RegistryInstance::getInstanceId).sorted().toList());
            assertTrue(directory.listInstances("prod", "payment-service").isEmpty());
            assertEquals(12L, directory.getDirectoryRevision());
        }
    }

    @Test
    void shouldDispatchWatchCallbacksOnProvidedExecutor() throws Exception {
        CountDownLatch eventLatch = new CountDownLatch(1);
        AtomicReference<String> callbackThreadName = new AtomicReference<>();
        AtomicReference<ServiceDirectorySubscription> subscriptionRef = new AtomicReference<>();
        ExecutorService callbackExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("starmap-test-callback");
            return thread;
        });
        executors.add(callbackExecutor);

        HttpServer server = startServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                writeSse(outputStream, """
                        id: 1
                        event: snapshot
                        data: {"revision":1,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                        
                        """);
                awaitSilently(200);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchAutoReconnect(false)
                .watchCallbackExecutor(callbackExecutor)
                .build();

        try (StellMapClient client = new StellMapClient(options);
             ServiceDirectorySubscription subscription = client.watchDirectory(RegistryWatchRequest.builder()
                     .namespace("prod")
                     .services(List.of("order-service"))
                     .build(), new RegistryWatchListener() {
                 @Override
                 public void onEvent(RegistryWatchEvent event) {
                     callbackThreadName.set(Thread.currentThread().getName());
                     eventLatch.countDown();
                     if (subscriptionRef.get() != null) {
                         subscriptionRef.get().close();
                     }
                 }
             })) {
            subscriptionRef.set(subscription);
            assertTrue(eventLatch.await(5, TimeUnit.SECONDS));
        }

        assertEquals("starmap-test-callback", callbackThreadName.get());
    }

    @Test
    void shouldScheduleHeartbeatPeriodicallyAndStopOnClose() throws Exception {
        AtomicInteger heartbeatCalls = new AtomicInteger();
        CountDownLatch heartbeatLatch = new CountDownLatch(2);
        ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        executors.add(heartbeatExecutor);

        HttpServer server = startServer(exchange -> {
            heartbeatCalls.incrementAndGet();
            heartbeatLatch.countDown();
            writeJson(exchange, 200, """
                    {"code":"ok","message":"heartbeat accepted"}
                    """);
        }, "/api/v1/registry/heartbeat");

        StellMapClientOptions options = options(server).toBuilder()
                .heartbeatExecutor(heartbeatExecutor)
                .build();

        try (StellMapClient client = new StellMapClient(options)) {
            HeartbeatSubscription subscription = client.scheduleHeartbeat(HeartbeatRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .leaseTtlSeconds(30)
                    .build(), Duration.ofMillis(100));

            assertTrue(heartbeatLatch.await(5, TimeUnit.SECONDS));

            subscription.close();
            int callsAfterClose = heartbeatCalls.get();
            awaitSilently(250);
            assertEquals(callsAfterClose, heartbeatCalls.get());
        }
    }

    @Test
    void shouldRegisterAndScheduleHeartbeatTogether() throws Exception {
        AtomicInteger registerCalls = new AtomicInteger();
        AtomicInteger heartbeatCalls = new AtomicInteger();
        CountDownLatch heartbeatLatch = new CountDownLatch(1);

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        executors.add(executor);
        server.createContext("/api/v1/registry/register", exchange -> {
            try {
                registerCalls.incrementAndGet();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"registered"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/api/v1/registry/heartbeat", exchange -> {
            try {
                heartbeatCalls.incrementAndGet();
                heartbeatLatch.countDown();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"heartbeat accepted"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.start();
        servers.add(server);

        try (StellMapClient client = new StellMapClient(options(server))) {
            try (HeartbeatSubscription ignored = client.registerAndScheduleHeartbeat(RegisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .leaseTtlSeconds(30)
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build(), Duration.ofMillis(100))) {
                assertTrue(heartbeatLatch.await(5, TimeUnit.SECONDS));
            }
        }

        assertEquals(1, registerCalls.get());
        assertTrue(heartbeatCalls.get() >= 1);
    }

    @Test
    void shouldRegisterAndScheduleHeartbeatWithDerivedDefaultInterval() throws Exception {
        AtomicInteger registerCalls = new AtomicInteger();
        AtomicInteger heartbeatCalls = new AtomicInteger();
        CountDownLatch heartbeatLatch = new CountDownLatch(1);
        AtomicReference<Long> firstHeartbeatElapsedMillis = new AtomicReference<>();
        AtomicReference<Long> startMillis = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        executors.add(executor);
        server.createContext("/api/v1/registry/register", exchange -> {
            try {
                registerCalls.incrementAndGet();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"registered"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/api/v1/registry/heartbeat", exchange -> {
            try {
                heartbeatCalls.incrementAndGet();
                if (firstHeartbeatElapsedMillis.get() == null && startMillis.get() != null) {
                    firstHeartbeatElapsedMillis.compareAndSet(null, System.currentTimeMillis() - startMillis.get());
                }
                heartbeatLatch.countDown();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"heartbeat accepted"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.start();
        servers.add(server);

        try (StellMapClient client = new StellMapClient(options(server))) {
            startMillis.set(System.currentTimeMillis());
            try (HeartbeatSubscription ignored = client.registerAndScheduleHeartbeat(RegisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .leaseTtlSeconds(1)
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build())) {
                assertTrue(heartbeatLatch.await(5, TimeUnit.SECONDS));
            }
        }

        assertEquals(1, registerCalls.get());
        assertTrue(heartbeatCalls.get() >= 1);
        assertNotNull(firstHeartbeatElapsedMillis.get());
        assertTrue(firstHeartbeatElapsedMillis.get() >= 900L);
        assertTrue(firstHeartbeatElapsedMillis.get() < 3000L);
    }

    @Test
    void shouldScheduleHeartbeatFromRegisterRequestWithDerivedDefaultInterval() throws Exception {
        AtomicInteger heartbeatCalls = new AtomicInteger();
        CountDownLatch heartbeatLatch = new CountDownLatch(1);
        AtomicReference<Long> firstHeartbeatElapsedMillis = new AtomicReference<>();
        AtomicReference<Long> startMillis = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            heartbeatCalls.incrementAndGet();
            if (firstHeartbeatElapsedMillis.get() == null && startMillis.get() != null) {
                firstHeartbeatElapsedMillis.compareAndSet(null, System.currentTimeMillis() - startMillis.get());
            }
            heartbeatLatch.countDown();
            writeJson(exchange, 200, """
                    {"code":"ok","message":"heartbeat accepted"}
                    """);
        }, "/api/v1/registry/heartbeat");

        try (StellMapClient client = new StellMapClient(options(server))) {
            startMillis.set(System.currentTimeMillis());
            try (HeartbeatSubscription ignored = client.scheduleHeartbeat(RegisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .leaseTtlSeconds(1)
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build())) {
                assertTrue(heartbeatLatch.await(5, TimeUnit.SECONDS));
            }
        }

        assertTrue(heartbeatCalls.get() >= 1);
        assertNotNull(firstHeartbeatElapsedMillis.get());
        assertTrue(firstHeartbeatElapsedMillis.get() >= 900L);
        assertTrue(firstHeartbeatElapsedMillis.get() < 3000L);
    }

    @Test
    void shouldMaintainDirectoryCacheWithoutListenerCallback() throws Exception {
        HttpServer server = startServer(exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                writeSse(outputStream, """
                        id: 1
                        event: snapshot
                        data: {"revision":1,"type":"SNAPSHOT","namespace":"prod","service":"order-service","instances":[{"namespace":"prod","service":"order-service","instanceId":"order-1","leaseTtlSeconds":30,"endpoints":[{"name":"http","protocol":"http","host":"127.0.0.1","port":8080,"weight":100}]}]}
                        
                        """);
                awaitSilently(300);
            }
        }, "/api/v1/registry/watch");

        StellMapClientOptions options = options(server).toBuilder()
                .watchAutoReconnect(false)
                .build();

        try (StellMapClient client = new StellMapClient(options);
             ServiceDirectorySubscription subscription = client.watchDirectory(RegistryWatchRequest.builder()
                     .namespace("prod")
                     .services(List.of("order-service"))
                     .includeSnapshot(true)
                     .build())) {
            assertTrue(waitUntil(() -> !subscription.getServiceDirectory().listInstances("prod", "order-service").isEmpty(), 3000));
            List<RegistryInstance> instances = subscription.getServiceDirectory().listInstances("prod", "order-service");
            assertEquals(1, instances.size());
            assertEquals("order-1", instances.getFirst().getInstanceId());
            assertEquals(1L, subscription.getLastRevision());
        }
    }

    @Test
    void shouldAutoDeregisterTrackedRegistrationsOnClose() throws Exception {
        AtomicInteger registerCalls = new AtomicInteger();
        AtomicInteger deregisterCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        executors.add(executor);
        server.createContext("/api/v1/registry/register", exchange -> {
            try {
                registerCalls.incrementAndGet();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"registered"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/api/v1/registry/deregister", exchange -> {
            try {
                deregisterCalls.incrementAndGet();
                writeJson(exchange, 200, """
                        {"code":"ok","message":"deregistered"}
                        """);
            } finally {
                exchange.close();
            }
        });
        server.start();
        servers.add(server);

        StellMapClientOptions options = options(server).toBuilder()
                .autoDeregisterOnClose(true)
                .build();

        try (StellMapClient client = new StellMapClient(options)) {
            client.register(RegisterRequest.builder()
                    .namespace("prod")
                    .service("order-service")
                    .instanceId("order-1")
                    .endpoints(List.of(Endpoint.builder()
                            .protocol("http")
                            .host("127.0.0.1")
                            .port(8080)
                            .build()))
                    .build());
        }

        assertEquals(1, registerCalls.get());
        assertEquals(1, deregisterCalls.get());
    }

    @Test
    void shouldSupportCustomNettyEventLoopOptionsAndOpenTelemetryConstructor() throws Exception {
        AtomicInteger requestCalls = new AtomicInteger();
        ExecutorService nettyExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("starmap-test-netty");
            return thread;
        });
        executors.add(nettyExecutor);

        HttpServer server = startServer(exchange -> {
            requestCalls.incrementAndGet();
            writeJson(exchange, 200, """
                    {"code":"ok","message":"instance registered"}
                    """);
        }, "/api/v1/registry/register");

        StellMapClientOptions options = options(server).toBuilder()
                .nettyEventLoopOptions(NettyEventLoopOptions.builder()
                        .threads(1)
                        .executor(nettyExecutor)
                        .build())
                .build();

        try (StellMapClient client = new StellMapClient(options, null, OpenTelemetry.noop())) {
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

        assertEquals(1, requestCalls.get());
    }

    @Test
    void shouldNormalizeBaseUrlAndDefaultHeadersBeforeSendingRequests() throws Exception {
        AtomicReference<String> capturedTraceHeader = new AtomicReference<>();
        AtomicReference<String> capturedIgnoredHeader = new AtomicReference<>();

        HttpServer server = startServer(exchange -> {
            capturedTraceHeader.set(exchange.getRequestHeaders().getFirst("X-Trace-Id"));
            capturedIgnoredHeader.set(exchange.getRequestHeaders().getFirst("X-Ignored"));
            writeJson(exchange, 200, """
                    {"code":"ok","message":"instance registered"}
                    """);
        }, "/api/v1/registry/register");

        StellMapClientOptions options = StellMapClientOptions.builder()
                .baseUrl("  " + baseUrl(server) + "  ")
                .requestTimeout(Duration.ofSeconds(2))
                .defaultHeaders(Map.of(
                        " X-Trace-Id ", " trace-1 ",
                        " X-Ignored ", "   "
                ))
                .build();

        try (StellMapClient client = new StellMapClient(options)) {
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

        assertEquals("trace-1", capturedTraceHeader.get());
        assertNull(capturedIgnoredHeader.get());
    }

    @Test
    void shouldRejectInvalidWatchReconnectDelayRange() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new StellMapClient(StellMapClientOptions.builder()
                        .baseUrl("http://127.0.0.1:8080")
                        .requestTimeout(Duration.ofSeconds(2))
                        .watchReconnectInitialDelay(Duration.ofSeconds(5))
                        .watchReconnectMaxDelay(Duration.ofSeconds(1))
                        .build()));

        assertEquals("watchReconnectMaxDelay must be greater than or equal to watchReconnectInitialDelay", exception.getMessage());
    }

    @Test
    void shouldThrowServerExceptionWhenFollowLeaderDisabled() throws Exception {
        HttpServer server = startServer(exchange -> writeJson(exchange, 503, """
                {"code":"not_leader","message":"redirect","leaderAddr":"127.0.0.1:18080"}
                """), "/api/v1/registry/register");

        StellMapClientOptions options = StellMapClientOptions.builder()
                .baseUrl(baseUrl(server))
                .requestTimeout(Duration.ofSeconds(2))
                .followLeaderRedirect(false)
                .build();

        try (StellMapClient client = new StellMapClient(options)) {
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
            assertInstanceOf(StellMapServerException.class, exception);
        }
    }

    private StellMapClientOptions options(HttpServer server) {
        return StellMapClientOptions.builder()
                .baseUrl(baseUrl(server))
                .requestTimeout(Duration.ofSeconds(2))
                .build();
    }

    private String baseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private HttpServer startServer(ExchangeHandler handler, String path) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        executors.add(executor);
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

    private void writeSse(OutputStream outputStream, String body) throws IOException {
        outputStream.write(body.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
    }

    private void awaitSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean waitUntil(Check check, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.matches()) {
                return true;
            }
            awaitSilently(20);
        }
        return check.matches();
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }

    @FunctionalInterface
    private interface Check {
        boolean matches();
    }
}
