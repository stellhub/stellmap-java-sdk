package io.github.starmap.sdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.starmap.sdk.exception.StarMapServerException;
import io.github.starmap.sdk.exception.StarMapTransportException;
import io.github.starmap.sdk.model.DeregisterRequest;
import io.github.starmap.sdk.model.Endpoint;
import io.github.starmap.sdk.model.HeartbeatRequest;
import io.github.starmap.sdk.model.RegisterRequest;
import io.github.starmap.sdk.model.RegistryInstance;
import io.github.starmap.sdk.model.RegistryQueryRequest;
import io.github.starmap.sdk.model.RegistryWatchEvent;
import io.github.starmap.sdk.model.StarMapErrorResponse;
import io.github.starmap.sdk.model.StarMapResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * StarMap Java SDK 主客户端。
 */
public class StarMapClient implements AutoCloseable {

    private static final String REGISTER_PATH = "/api/v1/registry/register";
    private static final String DEREGISTER_PATH = "/api/v1/registry/deregister";
    private static final String HEARTBEAT_PATH = "/api/v1/registry/heartbeat";
    private static final String INSTANCES_PATH = "/api/v1/registry/instances";
    private static final String WATCH_PATH = "/api/v1/registry/watch";

    private final URI baseUri;
    private final Duration requestTimeout;
    private final boolean followLeaderRedirect;
    private final int maxLeaderRedirects;
    private final Map<String, String> defaultHeaders;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ExecutorService watchExecutor;

    public StarMapClient(StarMapClientOptions options) {
        this(options, null);
    }

    public StarMapClient(StarMapClientOptions options, HttpClient httpClient) {
        Objects.requireNonNull(options, "options must not be null");
        this.baseUri = normalizeBaseUri(options.getBaseUrl());
        this.requestTimeout = normalizeTimeout(options.getRequestTimeout());
        this.followLeaderRedirect = options.isFollowLeaderRedirect();
        this.maxLeaderRedirects = Math.max(0, options.getMaxLeaderRedirects());
        this.defaultHeaders = normalizeHeaders(options.getDefaultHeaders());
        this.httpClient = httpClient != null ? httpClient : HttpClient.newBuilder()
                .connectTimeout(this.requestTimeout)
                .build();
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.watchExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("starmap-watch-", 0).factory());
    }

    /**
     * 注册服务实例。
     *
     * @param request 注册请求
     * @return StarMap 成功响应
     */
    public StarMapResponse<Void> register(RegisterRequest request) {
        JavaType responseType = objectMapper.getTypeFactory().constructParametricType(StarMapResponse.class, Void.class);
        return executeJson("POST", baseUri, REGISTER_PATH, normalizeRegisterRequest(request), responseType, true, maxLeaderRedirects);
    }

    /**
     * 注销服务实例。
     *
     * @param request 注销请求
     * @return StarMap 成功响应
     */
    public StarMapResponse<Void> deregister(DeregisterRequest request) {
        JavaType responseType = objectMapper.getTypeFactory().constructParametricType(StarMapResponse.class, Void.class);
        return executeJson("POST", baseUri, DEREGISTER_PATH, normalizeDeregisterRequest(request), responseType, true, maxLeaderRedirects);
    }

    /**
     * 发送实例心跳。
     *
     * @param request 心跳请求
     * @return StarMap 成功响应
     */
    public StarMapResponse<Void> heartbeat(HeartbeatRequest request) {
        JavaType responseType = objectMapper.getTypeFactory().constructParametricType(StarMapResponse.class, Void.class);
        return executeJson("POST", baseUri, HEARTBEAT_PATH, normalizeHeartbeatRequest(request), responseType, true, maxLeaderRedirects);
    }

    /**
     * 查询实例列表。
     *
     * @param request 查询条件
     * @return StarMap 成功响应
     */
    public StarMapResponse<List<RegistryInstance>> queryInstances(RegistryQueryRequest request) {
        RegistryQueryRequest normalized = normalizeRegistryQueryRequest(request);
        JavaType listType = objectMapper.getTypeFactory().constructCollectionType(List.class, RegistryInstance.class);
        JavaType responseType = objectMapper.getTypeFactory().constructParametricType(StarMapResponse.class, listType);
        return executeJson("GET", baseUri, INSTANCES_PATH + buildRegistryQuery(normalized), null, responseType, false, 0);
    }

    /**
     * 订阅实例 watch 事件。
     *
     * @param request 查询条件
     * @param listener watch 监听器
     * @return watch 订阅句柄
     */
    public RegistryWatchSubscription watchInstances(RegistryQueryRequest request, RegistryWatchListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        RegistryQueryRequest normalized = normalizeRegistryQueryRequest(request);

        HttpRequest.Builder builder = HttpRequest.newBuilder(resolve(baseUri, WATCH_PATH + buildRegistryQuery(normalized)))
                .timeout(requestTimeout)
                .header("Accept", "text/event-stream")
                .GET();
        defaultHeaders.forEach(builder::header);

        try {
            HttpResponse<InputStream> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
            if (!isSuccess(response.statusCode())) {
                throw buildServerException(response.statusCode(), readBody(response.body()));
            }
            listener.onOpen();
            Future<?> future = watchExecutor.submit(() -> runWatchLoop(response.body(), listener));
            return new DefaultRegistryWatchSubscription(response.body(), future, listener);
        } catch (IOException e) {
            throw new StarMapTransportException("Failed to open StarMap watch stream", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarMapTransportException("Interrupted while opening StarMap watch stream", e);
        }
    }

    @Override
    public void close() {
        watchExecutor.shutdownNow();
    }

    private void runWatchLoop(InputStream stream, RegistryWatchListener listener) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String eventName = null;
            String eventId = null;
            StringBuilder data = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    publishWatchEvent(eventName, eventId, data, listener);
                    eventName = null;
                    eventId = null;
                    data.setLength(0);
                    continue;
                }
                if (line.startsWith(":")) {
                    continue;
                }
                if (line.startsWith("event:")) {
                    eventName = line.substring("event:".length()).trim();
                    continue;
                }
                if (line.startsWith("id:")) {
                    eventId = line.substring("id:".length()).trim();
                    continue;
                }
                if (line.startsWith("data:")) {
                    if (data.length() > 0) {
                        data.append('\n');
                    }
                    data.append(line.substring("data:".length()).trim());
                }
            }
            publishWatchEvent(eventName, eventId, data, listener);
        } catch (IOException e) {
            listener.onError(new StarMapTransportException("Failed to read StarMap watch stream", e));
        } finally {
            listener.onClosed();
        }
    }

    private void publishWatchEvent(String eventName, String eventId, StringBuilder data, RegistryWatchListener listener) {
        if (data.length() == 0) {
            return;
        }
        try {
            RegistryWatchEvent event = objectMapper.readValue(data.toString(), RegistryWatchEvent.class);
            if (!hasText(event.getType()) && hasText(eventName)) {
                event.setType(eventName);
            }
            if (event.getRevision() == 0L && hasText(eventId)) {
                event.setRevision(Long.parseLong(eventId));
            }
            listener.onEvent(event);
        } catch (Exception e) {
            listener.onError(new StarMapTransportException("Failed to decode StarMap watch event", e));
        }
    }

    private <T> StarMapResponse<T> executeJson(
            String method,
            URI requestBaseUri,
            String path,
            Object requestBody,
            JavaType responseType,
            boolean enableLeaderRedirect,
            int redirectsRemaining
    ) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(resolve(requestBaseUri, path))
                .timeout(requestTimeout)
                .header("Accept", "application/json");
        defaultHeaders.forEach(builder::header);

        if ("GET".equals(method)) {
            builder.GET();
        } else {
            builder.header("Content-Type", "application/json");
            builder.method(method, HttpRequest.BodyPublishers.ofByteArray(toJsonBytes(requestBody)));
        }

        try {
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (isSuccess(response.statusCode())) {
                return objectMapper.readValue(response.body(), responseType);
            }

            StarMapServerException exception = buildServerException(response.statusCode(), response.body());
            StarMapErrorResponse errorResponse = exception.getErrorResponse();
            if (enableLeaderRedirect
                    && followLeaderRedirect
                    && redirectsRemaining > 0
                    && errorResponse != null
                    && "not_leader".equals(errorResponse.getCode())
                    && hasText(errorResponse.getLeaderAddr())) {
                return executeJson(method, toLeaderBaseUri(requestBaseUri, errorResponse.getLeaderAddr()), path, requestBody, responseType, true, redirectsRemaining - 1);
            }
            throw exception;
        } catch (JsonProcessingException e) {
            throw new StarMapTransportException("Failed to decode StarMap response", e);
        } catch (IOException e) {
            throw new StarMapTransportException("Failed to call StarMap HTTP API", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarMapTransportException("Interrupted while calling StarMap HTTP API", e);
        }
    }

    private String buildRegistryQuery(RegistryQueryRequest request) {
        List<String> items = new ArrayList<>();
        addQuery(items, "namespace", request.getNamespace());
        addQuery(items, "service", request.getService());
        addQuery(items, "zone", request.getZone());
        addQuery(items, "endpoint", request.getEndpoint());
        if (request.getLimit() != null && request.getLimit() > 0) {
            addQuery(items, "limit", String.valueOf(request.getLimit()));
        }
        if (request.getSelectors() != null) {
            for (String selector : request.getSelectors()) {
                addQuery(items, "selector", selector);
            }
        }
        if (request.getLabels() != null) {
            for (Map.Entry<String, String> entry : request.getLabels().entrySet()) {
                addQuery(items, "label", entry.getKey() + "=" + entry.getValue());
            }
        }
        return items.isEmpty() ? "" : "?" + String.join("&", items);
    }

    private void addQuery(List<String> items, String key, String value) {
        if (!hasText(value)) {
            return;
        }
        items.add(urlEncode(key) + "=" + urlEncode(value.trim()));
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private RegisterRequest normalizeRegisterRequest(RegisterRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getLeaseTtlSeconds() < 0) {
            throw new IllegalArgumentException("leaseTtlSeconds must be greater than or equal to 0");
        }
        List<Endpoint> endpoints = normalizeEndpoints(request.getEndpoints());
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one endpoint is required");
        }
        return RegisterRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(requireText(request.getService(), "service"))
                .instanceId(requireText(request.getInstanceId(), "instanceId"))
                .zone(trimToNull(request.getZone()))
                .labels(normalizeMap(request.getLabels()))
                .metadata(normalizeMap(request.getMetadata()))
                .endpoints(endpoints)
                .leaseTtlSeconds(request.getLeaseTtlSeconds() > 0 ? request.getLeaseTtlSeconds() : StarMapDefaults.DEFAULT_LEASE_TTL_SECONDS)
                .build();
    }

    private DeregisterRequest normalizeDeregisterRequest(DeregisterRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return DeregisterRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(requireText(request.getService(), "service"))
                .instanceId(requireText(request.getInstanceId(), "instanceId"))
                .build();
    }

    private HeartbeatRequest normalizeHeartbeatRequest(HeartbeatRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getLeaseTtlSeconds() < 0) {
            throw new IllegalArgumentException("leaseTtlSeconds must be greater than or equal to 0");
        }
        return HeartbeatRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(requireText(request.getService(), "service"))
                .instanceId(requireText(request.getInstanceId(), "instanceId"))
                .leaseTtlSeconds(request.getLeaseTtlSeconds())
                .build();
    }

    private RegistryQueryRequest normalizeRegistryQueryRequest(RegistryQueryRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getLimit() != null && request.getLimit() < 0) {
            throw new IllegalArgumentException("limit must be greater than or equal to 0");
        }
        return RegistryQueryRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(requireText(request.getService(), "service"))
                .zone(trimToNull(request.getZone()))
                .endpoint(trimToNull(request.getEndpoint()))
                .selectors(normalizeSelectors(request.getSelectors()))
                .labels(normalizeMap(request.getLabels()))
                .limit(request.getLimit())
                .build();
    }

    private List<Endpoint> normalizeEndpoints(List<Endpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return List.of();
        }
        Map<String, Endpoint> result = new LinkedHashMap<>();
        for (Endpoint endpoint : endpoints) {
            if (endpoint == null) {
                continue;
            }
            String protocol = requireText(endpoint.getProtocol(), "endpoint.protocol");
            String host = requireText(endpoint.getHost(), "endpoint.host");
            if (endpoint.getPort() <= 0 || endpoint.getPort() > 65535) {
                throw new IllegalArgumentException("endpoint.port must be within 1..65535");
            }
            if (endpoint.getWeight() < 0) {
                throw new IllegalArgumentException("endpoint.weight must be greater than or equal to 0");
            }
            String name = hasText(endpoint.getName()) ? endpoint.getName().trim() : protocol;
            if (result.containsKey(name)) {
                throw new IllegalArgumentException("duplicate endpoint name: " + name);
            }
            result.put(name, Endpoint.builder()
                    .name(name)
                    .protocol(protocol)
                    .host(host)
                    .port(endpoint.getPort())
                    .path(trimToNull(endpoint.getPath()))
                    .weight(endpoint.getWeight() > 0 ? endpoint.getWeight() : StarMapDefaults.DEFAULT_ENDPOINT_WEIGHT)
                    .build());
        }
        return List.copyOf(result.values());
    }

    private Map<String, String> normalizeMap(Map<String, String> source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        Map<String, String> result = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            String normalizedKey = trimToNull(key);
            if (normalizedKey != null) {
                result.put(normalizedKey, value == null ? "" : value.trim());
            }
        });
        return result.isEmpty() ? null : Map.copyOf(result);
    }

    private List<String> normalizeSelectors(List<String> selectors) {
        if (selectors == null || selectors.isEmpty()) {
            return null;
        }
        List<String> result = selectors.stream()
                .map(this::trimToNull)
                .filter(Objects::nonNull)
                .toList();
        return result.isEmpty() ? null : result;
    }

    private URI toLeaderBaseUri(URI currentBaseUri, String leaderAddr) {
        String normalized = leaderAddr.trim();
        if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            return normalizeBaseUri(normalized);
        }
        return normalizeBaseUri(currentBaseUri.getScheme() + "://" + normalized);
    }

    private URI normalizeBaseUri(String baseUrl) {
        String normalized = requireText(baseUrl, "baseUrl");
        if (!normalized.endsWith("/")) {
            normalized = normalized + "/";
        }
        return URI.create(normalized);
    }

    private Duration normalizeTimeout(Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be greater than zero");
        }
        return timeout;
    }

    private Map<String, String> normalizeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        Map<String, String> result = new LinkedHashMap<>();
        headers.forEach((key, value) -> {
            String normalizedKey = trimToNull(key);
            String normalizedValue = trimToNull(value);
            if (normalizedKey != null && normalizedValue != null) {
                result.put(normalizedKey, normalizedValue);
            }
        });
        return Map.copyOf(result);
    }

    private byte[] toJsonBytes(Object requestBody) {
        try {
            return objectMapper.writeValueAsBytes(requestBody);
        } catch (JsonProcessingException e) {
            throw new StarMapTransportException("Failed to encode StarMap request body", e);
        }
    }

    private StarMapServerException buildServerException(int statusCode, String body) {
        try {
            StarMapErrorResponse errorResponse = body == null || body.isBlank()
                    ? StarMapErrorResponse.builder().build()
                    : objectMapper.readValue(body, StarMapErrorResponse.class);
            return new StarMapServerException(statusCode, errorResponse);
        } catch (JsonProcessingException e) {
            return new StarMapServerException(statusCode, StarMapErrorResponse.builder().message(body).build());
        }
    }

    private String readBody(InputStream body) throws IOException {
        try (InputStream stream = body) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private URI resolve(URI currentBaseUri, String path) {
        return currentBaseUri.resolve(path.startsWith("/") ? path.substring(1) : path);
    }

    private boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private String requireText(String value, String fieldName) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static final class DefaultRegistryWatchSubscription implements RegistryWatchSubscription {

        private final InputStream stream;
        private final Future<?> future;
        private final RegistryWatchListener listener;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private DefaultRegistryWatchSubscription(InputStream stream, Future<?> future, RegistryWatchListener listener) {
            this.stream = stream;
            this.future = future;
            this.listener = listener;
        }

        @Override
        public boolean isClosed() {
            return closed.get() || future.isDone();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                stream.close();
            } catch (IOException e) {
                listener.onError(new StarMapTransportException("Failed to close StarMap watch stream", e));
            }
            future.cancel(true);
        }
    }
}
