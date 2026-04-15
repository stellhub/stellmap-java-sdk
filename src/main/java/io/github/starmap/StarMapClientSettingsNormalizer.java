package io.github.starmap;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 客户端基础配置归一化器。
 */
final class StarMapClientSettingsNormalizer {

    /**
     * 归一化客户端配置。
     *
     * @param options 原始客户端配置
     * @return 归一化后的客户端配置
     */
    NormalizedClientSettings normalize(StarMapClientOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        URI baseUri = normalizeBaseUri(options.getBaseUrl());
        Duration requestTimeout = normalizeTimeout(options.getRequestTimeout(), "requestTimeout");
        Duration watchReconnectInitialDelay = normalizeTimeout(options.getWatchReconnectInitialDelay(), "watchReconnectInitialDelay");
        Duration watchReconnectMaxDelay = normalizeTimeout(options.getWatchReconnectMaxDelay(), "watchReconnectMaxDelay");
        if (watchReconnectMaxDelay.compareTo(watchReconnectInitialDelay) < 0) {
            throw new IllegalArgumentException("watchReconnectMaxDelay must be greater than or equal to watchReconnectInitialDelay");
        }
        return new NormalizedClientSettings(
                baseUri,
                requestTimeout,
                options.isFollowLeaderRedirect(),
                Math.max(0, options.getMaxLeaderRedirects()),
                options.isAutoDeregisterOnClose(),
                options.isWatchAutoReconnect(),
                watchReconnectInitialDelay,
                watchReconnectMaxDelay,
                options.getWatchReconnectMaxAttempts(),
                normalizeHeaders(options.getDefaultHeaders())
        );
    }

    /**
     * 归一化任意超时时间。
     *
     * @param timeout 原始超时时间
     * @param fieldName 字段名
     * @return 归一化后的超时时间
     */
    Duration normalizeTimeout(Duration timeout, String fieldName) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero");
        }
        return timeout;
    }

    private URI normalizeBaseUri(String baseUrl) {
        String normalized = requireText(baseUrl, "baseUrl");
        if (!normalized.endsWith("/")) {
            normalized = normalized + "/";
        }
        return URI.create(normalized);
    }

    private Map<String, String> normalizeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        Map<String, String> result = new LinkedHashMap<>();
        headers.forEach((k, v) -> {
            String normalizedKey = trimToNull(k);
            String normalizedValue = trimToNull(v);
            if (normalizedKey != null && normalizedValue != null) {
                result.put(normalizedKey, normalizedValue);
            }
        });
        return Map.copyOf(result);
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

    /**
     * 归一化后的客户端配置结果。
     */
    record NormalizedClientSettings(
            URI baseUri,
            Duration requestTimeout,
            boolean followLeaderRedirect,
            int maxLeaderRedirects,
            boolean autoDeregisterOnClose,
            boolean watchAutoReconnect,
            Duration watchReconnectInitialDelay,
            Duration watchReconnectMaxDelay,
            int watchReconnectMaxAttempts,
            Map<String, String> defaultHeaders
    ) {
    }
}
