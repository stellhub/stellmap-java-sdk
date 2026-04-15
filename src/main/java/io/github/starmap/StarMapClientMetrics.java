package io.github.starmap;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

/** StarMap 客户端指标集合。 */
final class StarMapClientMetrics {

    private static final AttributeKey<String> ATTR_METHOD = AttributeKey.stringKey("starmap.method");
    private static final AttributeKey<String> ATTR_PATH = AttributeKey.stringKey("starmap.path");
    private static final AttributeKey<Long> ATTR_STATUS = AttributeKey.longKey("starmap.status_code");
    private static final AttributeKey<Boolean> ATTR_SUCCESS =
            AttributeKey.booleanKey("starmap.success");
    private static final AttributeKey<String> ATTR_EVENT_TYPE =
            AttributeKey.stringKey("starmap.event_type");
    private static final AttributeKey<Long> ATTR_ATTEMPT =
            AttributeKey.longKey("starmap.reconnect_attempt");
    private static final AttributeKey<String> ATTR_ERROR =
            AttributeKey.stringKey("starmap.error_type");
    private static final AttributeKey<Boolean> ATTR_SCHEDULED =
            AttributeKey.booleanKey("starmap.scheduled");

    private final LongCounter requestCounter;
    private final DoubleHistogram requestLatencyMillis;
    private final LongCounter watchEventCounter;
    private final LongCounter watchReconnectCounter;
    private final LongUpDownCounter activeWatchSubscriptions;
    private final LongUpDownCounter activeDirectoryServices;
    private final LongUpDownCounter activeDirectoryInstances;
    private final LongCounter autoDeregisterCounter;
    private final LongCounter heartbeatCounter;
    private final DoubleHistogram heartbeatLatencyMillis;
    private final LongUpDownCounter activeHeartbeatSchedules;
    private final LongUpDownCounter activeRegistrations;

    StarMapClientMetrics(
            OpenTelemetry openTelemetry, String instrumentationName, String instrumentationVersion) {
        Meter meter =
                openTelemetry
                        .getMeterProvider()
                        .meterBuilder(instrumentationName)
                        .setInstrumentationVersion(instrumentationVersion)
                        .build();
        this.requestCounter =
                meter
                        .counterBuilder("starmap_client_request_total")
                        .setDescription("Total StarMap client requests.")
                        .build();
        this.requestLatencyMillis =
                meter
                        .histogramBuilder("starmap_client_request_duration_ms")
                        .setUnit("ms")
                        .setDescription("StarMap client request duration in milliseconds.")
                        .build();
        this.watchEventCounter =
                meter
                        .counterBuilder("starmap_client_watch_event_total")
                        .setDescription("Total StarMap watch events.")
                        .build();
        this.watchReconnectCounter =
                meter
                        .counterBuilder("starmap_client_watch_reconnect_total")
                        .setDescription("Total StarMap watch reconnect attempts.")
                        .build();
        this.activeWatchSubscriptions =
                meter
                        .upDownCounterBuilder("starmap_client_watch_active")
                        .setDescription("Current active StarMap watch subscriptions.")
                        .build();
        this.activeDirectoryServices =
                meter
                        .upDownCounterBuilder("starmap_client_directory_service_active")
                        .setDescription("Current active services held in local watch caches.")
                        .build();
        this.activeDirectoryInstances =
                meter
                        .upDownCounterBuilder("starmap_client_directory_instance_active")
                        .setDescription("Current active instances held in local watch caches.")
                        .build();
        this.autoDeregisterCounter =
                meter
                        .counterBuilder("starmap_client_auto_deregister_total")
                        .setDescription("Total StarMap auto deregister attempts on close.")
                        .build();
        this.heartbeatCounter =
                meter
                        .counterBuilder("starmap_client_heartbeat_total")
                        .setDescription("Total StarMap heartbeats.")
                        .build();
        this.heartbeatLatencyMillis =
                meter
                        .histogramBuilder("starmap_client_heartbeat_duration_ms")
                        .setUnit("ms")
                        .setDescription("StarMap heartbeat duration in milliseconds.")
                        .build();
        this.activeHeartbeatSchedules =
                meter
                        .upDownCounterBuilder("starmap_client_heartbeat_schedule_active")
                        .setDescription("Current active StarMap scheduled heartbeats.")
                        .build();
        this.activeRegistrations =
                meter
                        .upDownCounterBuilder("starmap_client_registration_active")
                        .setDescription("Current active tracked StarMap registrations.")
                        .build();
    }

    void recordRequest(
            String method, String path, int statusCode, long durationNanos, boolean success) {
        Attributes attributes =
                Attributes.builder()
                        .put(ATTR_METHOD, method)
                        .put(ATTR_PATH, path)
                        .put(ATTR_STATUS, (long) statusCode)
                        .put(ATTR_SUCCESS, success)
                        .build();
        requestCounter.add(1L, attributes);
        requestLatencyMillis.record(durationNanos / 1_000_000D, attributes);
    }

    void recordWatchEvent(String eventType) {
        watchEventCounter.add(
                1L,
                Attributes.of(
                        ATTR_EVENT_TYPE,
                        eventType == null || eventType.isBlank() ? "unknown" : eventType.trim().toUpperCase()));
    }

    void recordWatchReconnect(int attempt, Throwable failure) {
        watchReconnectCounter.add(
                1L,
                Attributes.builder()
                        .put(ATTR_ATTEMPT, (long) attempt)
                        .put(ATTR_ERROR, failure == null ? "unknown" : failure.getClass().getSimpleName())
                        .build());
    }

    void recordWatchSubscriptionOpened() {
        activeWatchSubscriptions.add(1L);
    }

    void recordWatchSubscriptionClosed() {
        activeWatchSubscriptions.add(-1L);
    }

    void recordAutoDeregister(boolean success) {
        autoDeregisterCounter.add(1L, Attributes.of(ATTR_SUCCESS, success));
    }

    void recordHeartbeat(long durationNanos, boolean success, boolean scheduled) {
        Attributes attributes =
                Attributes.builder().put(ATTR_SUCCESS, success).put(ATTR_SCHEDULED, scheduled).build();
        heartbeatCounter.add(1L, attributes);
        heartbeatLatencyMillis.record(durationNanos / 1_000_000D, attributes);
    }

    void recordScheduledHeartbeatOpened() {
        activeHeartbeatSchedules.add(1L);
    }

    void recordScheduledHeartbeatClosed() {
        activeHeartbeatSchedules.add(-1L);
    }

    void recordRegistration(boolean register, boolean success) {
        if (!success) {
            return;
        }
        activeRegistrations.add(register ? 1L : -1L);
    }

    void recordDirectoryStateDelta(long serviceDelta, long instanceDelta) {
        if (serviceDelta != 0L) {
            activeDirectoryServices.add(serviceDelta);
        }
        if (instanceDelta != 0L) {
            activeDirectoryInstances.add(instanceDelta);
        }
    }
}
