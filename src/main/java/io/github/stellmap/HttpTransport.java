package io.github.stellmap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.stellmap.exception.StellMapServerException;
import io.github.stellmap.exception.StellMapTransportException;
import io.github.stellmap.model.StarMapErrorResponse;
import io.github.stellmap.model.StarMapResponse;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** HTTP 传输层，普通请求与 watch/SSE 都使用 OkHttp。 */
final class HttpTransport {

    private static final Logger log = LoggerFactory.getLogger(HttpTransport.class);
    private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json");

    private final ObjectMapper objectMapper;
    private final OkHttpClient okHttpClient;
    private final Map<String, String> defaultHeaders;
    private final StarMapClientMetrics metrics;
    private final boolean followLeaderRedirect;

    HttpTransport(
            ObjectMapper objectMapper,
            OkHttpClient okHttpClient,
            Map<String, String> defaultHeaders,
            StarMapClientMetrics metrics,
            boolean followLeaderRedirect) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.okHttpClient = Objects.requireNonNull(okHttpClient, "okHttpClient must not be null");
        this.defaultHeaders = Objects.requireNonNull(defaultHeaders, "defaultHeaders must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.followLeaderRedirect = followLeaderRedirect;
    }

    <T> StarMapResponse<T> executeJson(
            String method,
            URI requestBaseUri,
            String path,
            Object requestBody,
            JavaType responseType,
            boolean enableLeaderRedirect,
            int redirectsRemaining) {
        URI requestUri = resolve(requestBaseUri, path);
        byte[] bodyBytes = "GET".equals(method) ? null : toJsonBytes(requestBody);
        NettyHttpResponse response =
                executeRequest(method, requestUri, bodyBytes, "application/json", "application/json");

        try {
            if (isSuccess(response.statusCode())) {
                log.debug(
                        "StarMap request succeeded method={}, path={}, status={}",
                        method,
                        requestUri.getPath(),
                        response.statusCode());
                return objectMapper.readValue(response.body(), responseType);
            }

            StellMapServerException exception =
                    buildServerException(response.statusCode(), response.body());
            StarMapErrorResponse errorResponse = exception.getErrorResponse();
            if (enableLeaderRedirect
                    && followLeaderRedirect
                    && redirectsRemaining > 0
                    && errorResponse != null
                    && "not_leader".equals(errorResponse.getCode())
                    && hasText(errorResponse.getLeaderAddr())) {
                log.info(
                        "Redirecting StarMap write request to leader method={}, path={}, leaderAddr={},"
                                + " redirectsRemaining={}",
                        method,
                        requestUri.getPath(),
                        errorResponse.getLeaderAddr(),
                        redirectsRemaining);
                return executeJson(
                        method,
                        toLeaderBaseUri(requestBaseUri, errorResponse.getLeaderAddr()),
                        path,
                        requestBody,
                        responseType,
                        true,
                        redirectsRemaining - 1);
            }
            log.warn(
                    "StarMap request failed method={}, path={}, status={}, code={}",
                    method,
                    requestUri.getPath(),
                    response.statusCode(),
                    errorResponse == null ? null : errorResponse.getCode());
            throw exception;
        } catch (JsonProcessingException e) {
            log.warn(
                    "Failed to decode StarMap response method={}, path={}", method, requestUri.getPath(), e);
            throw new StellMapTransportException("Failed to decode StarMap response", e);
        }
    }

    Request buildWatchRequest(URI requestUri) {
        Request.Builder builder = new Request.Builder().url(requestUri.toString());
        builder.get();
        builder.header("Accept", "text/event-stream");
        builder.header("Cache-Control", "no-cache");
        builder.header("Connection", "keep-alive");
        defaultHeaders.forEach(builder::addHeader);
        return builder.build();
    }

    Call newCall(Request request) {
        return okHttpClient.newCall(request);
    }

    void close() {
        okHttpClient.dispatcher().cancelAll();
        okHttpClient.dispatcher().executorService().shutdown();
        okHttpClient.connectionPool().evictAll();
    }

    URI resolve(URI currentBaseUri, String path) {
        return currentBaseUri.resolve(path.startsWith("/") ? path.substring(1) : path);
    }

    StellMapServerException buildServerException(int statusCode, String body) {
        try {
            StarMapErrorResponse errorResponse =
                    body == null || body.isBlank()
                            ? StarMapErrorResponse.builder().build()
                            : objectMapper.readValue(body, StarMapErrorResponse.class);
            return new StellMapServerException(statusCode, errorResponse);
        } catch (JsonProcessingException e) {
            return new StellMapServerException(
                    statusCode, StarMapErrorResponse.builder().message(body).build());
        }
    }

    boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    StellMapTransportException unwrapAsTransport(Throwable cause, String message) {
        if (cause instanceof StellMapServerException serverException) {
            throw serverException;
        }
        if (cause instanceof StellMapTransportException transportException) {
            return transportException;
        }
        return new StellMapTransportException(message, cause);
    }

    private NettyHttpResponse executeRequest(
            String method,
            URI requestUri,
            byte[] requestBody,
            String acceptHeader,
            String contentTypeHeader) {
        long startNanos = System.nanoTime();
        Request request = buildJsonRequest(method, requestUri, requestBody, acceptHeader, contentTypeHeader);
        try (Response response = okHttpClient.newCall(request).execute()) {
            NettyHttpResponse httpResponse =
                    new NettyHttpResponse(response.code(), readResponseBody(response.body()));
            metrics.recordRequest(
                    method,
                    requestUri.getPath(),
                    httpResponse.statusCode(),
                    System.nanoTime() - startNanos,
                    isSuccess(httpResponse.statusCode()));
            return httpResponse;
        } catch (IOException e) {
            metrics.recordRequest(method, requestUri.getPath(), 0, System.nanoTime() - startNanos, false);
            log.warn(
                    "StarMap request execution failed method={}, uri={}", method, requestUri, e);
            throw new StellMapTransportException("Failed to call StarMap HTTP API", e);
        } catch (RuntimeException e) {
            metrics.recordRequest(method, requestUri.getPath(), 0, System.nanoTime() - startNanos, false);
            throw unwrapAsTransport(e, "Failed to call StarMap HTTP API");
        }
    }

    private Request buildJsonRequest(
            String method,
            URI requestUri,
            byte[] requestBody,
            String acceptHeader,
            String contentTypeHeader) {
        Request.Builder builder = new Request.Builder().url(requestUri.toString());
        builder.header("Accept", acceptHeader);
        defaultHeaders.forEach(builder::addHeader);

        RequestBody body = null;
        if (!"GET".equalsIgnoreCase(method)) {
            body = RequestBody.create(requestBody == null ? new byte[0] : requestBody, JSON_MEDIA_TYPE);
            builder.header("Content-Type", contentTypeHeader);
        }
        builder.method(method, body);
        return builder.build();
    }

    private String readResponseBody(ResponseBody responseBody) throws IOException {
        if (responseBody == null) {
            return "";
        }
        byte[] bytes = responseBody.bytes();
        if (bytes.length > MAX_RESPONSE_BYTES) {
            throw new StellMapTransportException("StarMap response body exceeded max size");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private URI toLeaderBaseUri(URI currentBaseUri, String leaderAddr) {
        String normalized = leaderAddr.trim();
        if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            return normalizeBaseUri(normalized);
        }
        return normalizeBaseUri(currentBaseUri.getScheme() + "://" + normalized);
    }

    private URI normalizeBaseUri(String baseUrl) {
        String normalized = baseUrl.trim();
        if (!normalized.endsWith("/")) {
            normalized = normalized + "/";
        }
        return URI.create(normalized);
    }

    private byte[] toJsonBytes(Object requestBody) {
        try {
            return objectMapper.writeValueAsBytes(requestBody);
        } catch (JsonProcessingException e) {
            throw new StellMapTransportException("Failed to encode StarMap request body", e);
        }
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private record NettyHttpResponse(int statusCode, String body) {}
}
