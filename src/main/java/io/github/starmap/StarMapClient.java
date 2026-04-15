package io.github.starmap;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.starmap.model.DeregisterRequest;
import io.github.starmap.model.HeartbeatRequest;
import io.github.starmap.model.RegisterRequest;
import io.github.starmap.model.RegistryInstance;
import io.github.starmap.model.RegistryQueryRequest;
import io.github.starmap.model.RegistryWatchEvent;
import io.github.starmap.model.RegistryWatchRequest;
import io.github.starmap.model.StarMapResponse;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** StarMap Java SDK 主客户端。 */
public class StarMapClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StarMapClient.class);

    private static final String REGISTER_PATH = "/api/v1/registry/register";
    private static final String DEREGISTER_PATH = "/api/v1/registry/deregister";
    private static final String INSTANCES_PATH = "/api/v1/registry/instances";
    private static final String INSTRUMENTATION_NAME = "io.github.starmap";
    private static final String INSTRUMENTATION_VERSION = "0.0.1";
    private static final NettyClientFactory NETTY_CLIENT_FACTORY = new NettyClientFactory();
    private static final StarMapClientSettingsNormalizer SETTINGS_NORMALIZER =
            new StarMapClientSettingsNormalizer();
    static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final StarMapRequestNormalizer REQUEST_NORMALIZER = new StarMapRequestNormalizer();

    private final URI baseUri;
    private final int maxLeaderRedirects;
    private final boolean watchAutoReconnect;
    private final Duration watchReconnectInitialDelay;
    private final Duration watchReconnectMaxDelay;
    private final int watchReconnectMaxAttempts;
    private final StarMapClientMetrics metrics;
    private final EventLoopGroup eventLoopGroup;
    private final ExecutorService watchCallbackExecutor;
    private final NettyHttpTransport httpTransport;
    private final StarMapHeartbeatManager heartbeatManager;
    private final StarMapClientLifecycleManager lifecycleManager;
    private static final RegistryWatchListener NOOP_WATCH_LISTENER =
            new RegistryWatchListener() {
                @Override
                public void onEvent(RegistryWatchEvent event) {}
            };

    public StarMapClient(StarMapClientOptions options) {
        this(options, null, null);
    }

    public StarMapClient(StarMapClientOptions options, OpenTelemetry openTelemetry) {
        this(options, null, openTelemetry);
    }

    /**
     * 基于 Netty EventLoopGroup 构建客户端。
     *
     * @param options 客户端配置
     * @param eventLoopGroup 可选外部 EventLoopGroup
     */
    public StarMapClient(StarMapClientOptions options, EventLoopGroup eventLoopGroup) {
        this(options, eventLoopGroup, null);
    }

    /**
     * 基于 Netty EventLoopGroup 与 OpenTelemetry 构建客户端。
     *
     * @param options 客户端配置
     * @param eventLoopGroup 可选外部 EventLoopGroup
     * @param openTelemetry 可选 OpenTelemetry
     */
    public StarMapClient(
            StarMapClientOptions options, EventLoopGroup eventLoopGroup, OpenTelemetry openTelemetry) {
        StarMapClientSettingsNormalizer.NormalizedClientSettings normalizedSettings =
                SETTINGS_NORMALIZER.normalize(options);
        this.baseUri = normalizedSettings.baseUri();
        this.maxLeaderRedirects = normalizedSettings.maxLeaderRedirects();
        this.watchAutoReconnect = normalizedSettings.watchAutoReconnect();
        this.watchReconnectInitialDelay = normalizedSettings.watchReconnectInitialDelay();
        this.watchReconnectMaxDelay = normalizedSettings.watchReconnectMaxDelay();
        this.watchReconnectMaxAttempts = normalizedSettings.watchReconnectMaxAttempts();
        OpenTelemetry effectiveOpenTelemetry =
                openTelemetry != null ? openTelemetry : GlobalOpenTelemetry.get();
        this.metrics =
                new StarMapClientMetrics(
                        effectiveOpenTelemetry, INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION);
        NettyClientFactory.ClientResources clientResources =
                NETTY_CLIENT_FACTORY.createResources(
                        new NettyClientFactory.ClientSettings(
                                this.baseUri,
                                normalizedSettings.requestTimeout(),
                                normalizedSettings.followLeaderRedirect(),
                                this.maxLeaderRedirects,
                                normalizedSettings.autoDeregisterOnClose(),
                                normalizedSettings.defaultHeaders(),
                                eventLoopGroup,
                                options.getWatchCallbackExecutor(),
                                options.getHeartbeatExecutor(),
                                options.getNettyEventLoopOptions(),
                                this.metrics));
        this.eventLoopGroup = clientResources.eventLoopGroup();
        this.watchCallbackExecutor = clientResources.watchCallbackExecutor();
        this.httpTransport = clientResources.httpTransport();
        this.heartbeatManager = clientResources.heartbeatManager();
        this.lifecycleManager = clientResources.lifecycleManager();
        log.debug(
                "Initialized StarMapClient baseUrl={}, autoDeregisterOnClose={}, watchAutoReconnect={}",
                this.baseUri,
                normalizedSettings.autoDeregisterOnClose(),
                this.watchAutoReconnect);
    }

    /**
     * 注册服务实例。
     *
     * @param request 注册请求
     * @return StarMap 成功响应
     */
    public StarMapResponse<Void> register(RegisterRequest request) {
        JavaType responseType =
                OBJECT_MAPPER.getTypeFactory().constructParametricType(StarMapResponse.class, Void.class);
        RegisterRequest normalized = REQUEST_NORMALIZER.normalizeRegisterRequest(request);
        StarMapResponse<Void> response =
                httpTransport.executeJson(
                        "POST", baseUri, REGISTER_PATH, normalized, responseType, true, maxLeaderRedirects);
        heartbeatManager.trackRegistration(normalized);
        log.info(
                "Registered instance namespace={}, service={}, instanceId={}",
                normalized.getNamespace(),
                normalized.getService(),
                normalized.getInstanceId());
        return response;
    }

    /**
     * 注册服务实例并启动定时心跳。
     *
     * @param request 注册请求
     * @return 定时心跳句柄
     */
    public HeartbeatSubscription registerAndScheduleHeartbeat(RegisterRequest request) {
        RegisterRequest normalized = REQUEST_NORMALIZER.normalizeRegisterRequest(request);
        register(normalized);
        return scheduleHeartbeat(
                toHeartbeatRequest(normalized), resolveHeartbeatInterval(normalized.getLeaseTtlSeconds()));
    }

    /**
     * 注册服务实例并按指定间隔启动定时心跳。
     *
     * @param request 注册请求
     * @param interval 心跳间隔
     * @return 定时心跳句柄
     */
    public HeartbeatSubscription registerAndScheduleHeartbeat(
            RegisterRequest request, Duration interval) {
        RegisterRequest normalized = REQUEST_NORMALIZER.normalizeRegisterRequest(request);
        register(normalized);
        return scheduleHeartbeat(toHeartbeatRequest(normalized), interval);
    }

    /**
     * 注销服务实例。
     *
     * @param request 注销请求
     * @return StarMap 成功响应
     */
    public StarMapResponse<Void> deregister(DeregisterRequest request) {
        JavaType responseType =
                OBJECT_MAPPER.getTypeFactory().constructParametricType(StarMapResponse.class, Void.class);
        DeregisterRequest normalized = REQUEST_NORMALIZER.normalizeDeregisterRequest(request);
        StarMapResponse<Void> response =
                httpTransport.executeJson(
                        "POST", baseUri, DEREGISTER_PATH, normalized, responseType, true, maxLeaderRedirects);
        heartbeatManager.handleDeregistered(normalized);
        log.info(
                "Deregistered instance namespace={}, service={}, instanceId={}",
                normalized.getNamespace(),
                normalized.getService(),
                normalized.getInstanceId());
        return response;
    }

    /**
     * 发送实例心跳。
     *
     * @param request 心跳请求
     * @return StarMap 成功响应
     */
    public StarMapResponse<Void> heartbeat(HeartbeatRequest request) {
        HeartbeatRequest normalized = REQUEST_NORMALIZER.normalizeHeartbeatRequest(request);
        StarMapResponse<Void> response = heartbeatManager.heartbeat(normalized);
        log.debug(
                "Heartbeat succeeded namespace={}, service={}, instanceId={}",
                normalized.getNamespace(),
                normalized.getService(),
                normalized.getInstanceId());
        return response;
    }

    /**
     * 启动定时心跳。
     *
     * @param request 心跳请求
     * @return 定时心跳句柄
     */
    public HeartbeatSubscription scheduleHeartbeat(HeartbeatRequest request) {
        HeartbeatRequest normalized =
                REQUEST_NORMALIZER.normalizeHeartbeatRequestForScheduling(request);
        return heartbeatManager.scheduleHeartbeat(
                normalized, resolveHeartbeatInterval(normalized.getLeaseTtlSeconds()));
    }

    /**
     * 使用指定间隔启动定时心跳。
     *
     * @param request 心跳请求
     * @param interval 心跳间隔
     * @return 定时心跳句柄
     */
    public HeartbeatSubscription scheduleHeartbeat(HeartbeatRequest request, Duration interval) {
        HeartbeatRequest normalized =
                REQUEST_NORMALIZER.normalizeHeartbeatRequestForScheduling(request);
        return heartbeatManager.scheduleHeartbeat(
                normalized, SETTINGS_NORMALIZER.normalizeTimeout(interval, "interval"));
    }

    /**
     * 根据注册请求启动定时心跳。
     *
     * @param request 注册请求
     * @return 定时心跳句柄
     */
    public HeartbeatSubscription scheduleHeartbeat(RegisterRequest request) {
        RegisterRequest normalized = REQUEST_NORMALIZER.normalizeRegisterRequest(request);
        return scheduleHeartbeat(normalized, resolveHeartbeatInterval(normalized.getLeaseTtlSeconds()));
    }

    /**
     * 将注册请求转换为心跳请求。
     *
     * @param request 已归一化的注册请求
     * @return 对应的心跳请求
     */
    private HeartbeatRequest toHeartbeatRequest(RegisterRequest request) {
        return HeartbeatRequest.builder()
                .namespace(request.getNamespace())
                .service(request.getService())
                .organization(request.getOrganization())
                .businessDomain(request.getBusinessDomain())
                .capabilityDomain(request.getCapabilityDomain())
                .application(request.getApplication())
                .role(request.getRole())
                .instanceId(request.getInstanceId())
                .leaseTtlSeconds(request.getLeaseTtlSeconds())
                .build();
    }

    /**
     * 根据注册请求和显式间隔启动定时心跳。
     *
     * @param request 注册请求
     * @param interval 心跳间隔
     * @return 定时心跳句柄
     */
    public HeartbeatSubscription scheduleHeartbeat(RegisterRequest request, Duration interval) {
        RegisterRequest normalized = REQUEST_NORMALIZER.normalizeRegisterRequest(request);
        return scheduleHeartbeat(toHeartbeatRequest(normalized), interval);
    }

    /**
     * 查询实例列表。
     *
     * @param request 查询条件
     * @return StarMap 成功响应
     */
    public StarMapResponse<List<RegistryInstance>> queryInstances(RegistryQueryRequest request) {
        RegistryQueryRequest normalized = REQUEST_NORMALIZER.normalizeRegistryQueryRequest(request);
        JavaType listType =
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, RegistryInstance.class);
        JavaType responseType =
                OBJECT_MAPPER.getTypeFactory().constructParametricType(StarMapResponse.class, listType);
        return httpTransport.executeJson(
                "GET",
                baseUri,
                INSTANCES_PATH + REQUEST_NORMALIZER.buildRegistryQuery(normalized),
                null,
                responseType,
                false,
                0);
    }

    /**
     * 订阅单个服务实例 watch 事件。
     *
     * @param request 查询条件
     * @param listener watch 监听器
     * @return watch 订阅句柄
     */
    public RegistryWatchSubscription watchInstances(
            RegistryQueryRequest request, RegistryWatchListener listener) {
        RegistryQueryRequest normalized = REQUEST_NORMALIZER.normalizeRegistryQueryRequest(request);
        return watchDirectory(REQUEST_NORMALIZER.toWatchRequest(normalized), listener);
    }

    /**
     * 订阅聚合目录变更，并自动维护本地缓存。
     *
     * @param request 聚合 watch 请求
     * @return 目录订阅句柄。该重载不派发业务回调，只维护本地目录缓存。
     */
    public ServiceDirectorySubscription watchDirectory(RegistryWatchRequest request) {
        return watchDirectory(request, NOOP_WATCH_LISTENER);
    }

    /**
     * 订阅聚合目录变更，并自动维护本地缓存。
     *
     * @param request 聚合 watch 请求
     * @param listener 事件监听器
     * @return 目录订阅句柄
     */
    public ServiceDirectorySubscription watchDirectory(
            RegistryWatchRequest request, RegistryWatchListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        RegistryWatchRequest normalized = REQUEST_NORMALIZER.normalizeWatchRequest(request);
        ManagedDirectoryWatchSubscription subscription =
                new ManagedDirectoryWatchSubscription(this, normalized, listener);
        subscription.openInitial();
        return subscription;
    }

    URI baseUriInternal() {
        return baseUri;
    }

    EventLoopGroup eventLoopGroupInternal() {
        return eventLoopGroup;
    }

    ExecutorService watchCallbackExecutorInternal() {
        return watchCallbackExecutor;
    }

    boolean watchAutoReconnectEnabled() {
        return watchAutoReconnect;
    }

    Duration watchReconnectInitialDelayInternal() {
        return watchReconnectInitialDelay;
    }

    Duration watchReconnectMaxDelayInternal() {
        return watchReconnectMaxDelay;
    }

    int watchReconnectMaxAttemptsInternal() {
        return watchReconnectMaxAttempts;
    }

    StarMapClientMetrics clientMetrics() {
        return metrics;
    }

    NettyHttpTransport transportInternal() {
        return httpTransport;
    }

    StarMapRequestNormalizer requestNormalizerInternal() {
        return REQUEST_NORMALIZER;
    }

    @Override
    public void close() {
        lifecycleManager.close();
    }

    boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private Duration resolveHeartbeatInterval(long leaseTtlSeconds) {
        long ttl = leaseTtlSeconds > 0L ? leaseTtlSeconds : StarMapDefaults.DEFAULT_LEASE_TTL_SECONDS;
        long intervalSeconds = Math.max(1L, ttl / 3L);
        return Duration.ofSeconds(intervalSeconds);
    }
}
