package io.github.starmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.starmap.model.DeregisterRequest;
import io.github.starmap.model.Endpoint;
import io.github.starmap.model.HeartbeatRequest;
import io.github.starmap.model.RegisterRequest;
import io.github.starmap.model.RegistryInstance;
import io.github.starmap.model.RegistryQueryRequest;
import io.github.starmap.model.RegistryWatchEvent;
import io.github.starmap.model.RegistryWatchRequest;
import io.github.starmap.model.StarMapResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** 连接本地真实 StarMap 的手动集成测试。 */
@Disabled("Requires a locally running StarMap server and manual execution.")
class DisabledClientTest {

    private static final String LOCALHOST =
            System.getProperty("starmap.local.baseUrl", "http://127.0.0.1:8080");

    @Test
    void shouldRegisterQueryHeartbeatAndDeregisterAgainstLocalStarMap() {
        String suffix = uniqueSuffix();
        String namespace = "manual-" + suffix;
        String service = "order-service-" + suffix;
        String instanceId = "instance-" + suffix;

        RegisterRequest registerRequest =
                RegisterRequest.builder()
                        .namespace(namespace)
                        .service(service)
                        .instanceId(instanceId)
                        .zone("az1")
                        .labels(Map.of("color", "gray"))
                        .metadata(Map.of("owner", "trade-team"))
                        .endpoints(List.of(httpEndpoint(18080)))
                        .build();

        try (StarMapClient client = new StarMapClient(options())) {
            StarMapResponse<Void> registerResponse = client.register(registerRequest);
            assertEquals("ok", registerResponse.getCode());

            assertTrue(waitUntil(() -> containsInstance(client, namespace, service, instanceId), 5000));

            StarMapResponse<Void> heartbeatResponse =
                    client.heartbeat(
                            HeartbeatRequest.builder()
                                    .namespace(namespace)
                                    .service(service)
                                    .instanceId(instanceId)
                                    .leaseTtlSeconds(registerRequest.getLeaseTtlSeconds())
                                    .build());
            assertEquals("ok", heartbeatResponse.getCode());

            StarMapResponse<Void> deregisterResponse =
                    client.deregister(
                            DeregisterRequest.builder()
                                    .namespace(namespace)
                                    .service(service)
                                    .instanceId(instanceId)
                                    .build());
            assertEquals("ok", deregisterResponse.getCode());

            assertTrue(waitUntil(() -> !containsInstance(client, namespace, service, instanceId), 5000));
        }
    }

    @Test
    void shouldComposeStructuredServiceIdentityAcrossRequestsAgainstLocalStarMap() {
        String suffix = uniqueSuffix();
        String namespace = "manual-" + suffix;
        String application = "order-center-" + suffix;
        String instanceId = "instance-" + suffix;

        RegisterRequest registerRequest =
                RegisterRequest.builder()
                        .namespace(namespace)
                        .organization("company")
                        .businessDomain("trade")
                        .capabilityDomain("order")
                        .application(application)
                        .role("api")
                        .instanceId(instanceId)
                        .endpoints(List.of(httpEndpoint(18081)))
                        .build();

        try (StarMapClient client = new StarMapClient(options())) {
            StarMapResponse<Void> registerResponse = client.register(registerRequest);
            assertEquals("ok", registerResponse.getCode());

            String expectedService = "company.trade.order." + application + ".api";
            assertTrue(
                    waitUntil(() -> containsInstance(client, namespace, expectedService, instanceId), 5000));

            StarMapResponse<List<RegistryInstance>> queryResponse =
                    client.queryInstances(
                            RegistryQueryRequest.builder()
                                    .namespace(namespace)
                                    .organization("company")
                                    .businessDomain("trade")
                                    .capabilityDomain("order")
                                    .application(application)
                                    .role("api")
                                    .build());

            assertNotNull(queryResponse.getData());
            assertFalse(queryResponse.getData().isEmpty());
            assertTrue(
                    queryResponse.getData().stream()
                            .anyMatch(
                                    instance ->
                                            expectedService.equals(instance.getService())
                                                    && instanceId.equals(instance.getInstanceId())));

            StarMapResponse<Void> deregisterResponse =
                    client.deregister(
                            DeregisterRequest.builder()
                                    .namespace(namespace)
                                    .organization("company")
                                    .businessDomain("trade")
                                    .capabilityDomain("order")
                                    .application(application)
                                    .role("api")
                                    .instanceId(instanceId)
                                    .build());
            assertEquals("ok", deregisterResponse.getCode());
        }
    }

    @Test
    void shouldWatchDirectoryChangesAgainstLocalStarMap() throws Exception {
        String suffix = uniqueSuffix();
        String namespace = "manual-" + suffix;
        String service = "watch-service-" + suffix;
        String instanceId = "instance-" + suffix;
        CountDownLatch eventLatch = new CountDownLatch(1);
        CopyOnWriteArrayList<RegistryWatchEvent> receivedEvents = new CopyOnWriteArrayList<>();
        AtomicReference<ServiceDirectorySubscription> subscriptionRef = new AtomicReference<>();

        RegisterRequest registerRequest =
                RegisterRequest.builder()
                        .namespace(namespace)
                        .service(service)
                        .instanceId(instanceId)
                        .endpoints(List.of(httpEndpoint(18082)))
                        .build();

        try (StarMapClient client = new StarMapClient(options());
                ServiceDirectorySubscription subscription =
                        client.watchDirectory(
                                RegistryWatchRequest.builder()
                                        .namespace(namespace)
                                        .services(List.of(service))
                                        .includeSnapshot(true)
                                        .build(),
                                new RegistryWatchListener() {
                                    @Override
                                    public void onEvent(RegistryWatchEvent event) {
                                        receivedEvents.add(event);
                                        if (subscriptionRef.get() != null
                                                && !subscriptionRef
                                                        .get()
                                                        .getServiceDirectory()
                                                        .listInstances(namespace, service)
                                                        .isEmpty()) {
                                            eventLatch.countDown();
                                        }
                                    }
                                })) {
            subscriptionRef.set(subscription);
            awaitSilently(300);

            StarMapResponse<Void> registerResponse = client.register(registerRequest);
            assertEquals("ok", registerResponse.getCode());

            assertTrue(eventLatch.await(10, TimeUnit.SECONDS));
            assertTrue(
                    waitUntil(
                            () -> !subscription.getServiceDirectory().listInstances(namespace, service).isEmpty(),
                            5000));
            assertTrue(
                    receivedEvents.stream()
                            .anyMatch(
                                    event -> service.equals(event.getService()) || event.getInstances() != null));

            StarMapResponse<Void> deregisterResponse =
                    client.deregister(
                            DeregisterRequest.builder()
                                    .namespace(namespace)
                                    .service(service)
                                    .instanceId(instanceId)
                                    .build());
            assertEquals("ok", deregisterResponse.getCode());
        }
    }

    @Test
    void shouldRegisterAndScheduleHeartbeatAgainstLocalStarMap() throws Exception {
        String suffix = uniqueSuffix();
        String namespace = "manual-" + suffix;
        String service = "heartbeat-service-" + suffix;
        String instanceId = "instance-" + suffix;

        RegisterRequest registerRequest =
                RegisterRequest.builder()
                        .namespace(namespace)
                        .service(service)
                        .instanceId(instanceId)
                        .leaseTtlSeconds(6)
                        .endpoints(List.of(httpEndpoint(18083)))
                        .build();

        try (StarMapClient client = new StarMapClient(options());
                HeartbeatSubscription subscription =
                        client.registerAndScheduleHeartbeat(registerRequest, Duration.ofSeconds(1))) {
            assertFalse(subscription.isClosed());
            awaitSilently(3500);
            assertTrue(containsInstance(client, namespace, service, instanceId));
        } finally {
            cleanup(namespace, service, instanceId);
        }
    }

    private StarMapClientOptions options() {
        return StarMapClientOptions.builder()
                .baseUrl(LOCALHOST)
                .requestTimeout(Duration.ofSeconds(5))
                .watchReconnectInitialDelay(Duration.ofMillis(200))
                .watchReconnectMaxDelay(Duration.ofSeconds(1))
                .build();
    }

    private Endpoint httpEndpoint(int port) {
        return Endpoint.builder().protocol("http").host("127.0.0.1").port(port).build();
    }

    private boolean containsInstance(
            StarMapClient client, String namespace, String service, String instanceId) {
        StarMapResponse<List<RegistryInstance>> response =
                client.queryInstances(
                        RegistryQueryRequest.builder().namespace(namespace).service(service).build());
        return response.getData() != null
                && response.getData().stream()
                        .anyMatch(instance -> instanceId.equals(instance.getInstanceId()));
    }

    private void cleanup(String namespace, String service, String instanceId) {
        try (StarMapClient client = new StarMapClient(options())) {
            client.deregister(
                    DeregisterRequest.builder()
                            .namespace(namespace)
                            .service(service)
                            .instanceId(instanceId)
                            .build());
        } catch (Exception ignored) {
        }
    }

    private boolean waitUntil(Check check, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (check.matches()) {
                return true;
            }
            awaitSilently(100);
        }
        return check.matches();
    }

    private void awaitSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String uniqueSuffix() {
        return Long.toString(System.nanoTime());
    }

    @FunctionalInterface
    private interface Check {
        boolean matches();
    }
}
