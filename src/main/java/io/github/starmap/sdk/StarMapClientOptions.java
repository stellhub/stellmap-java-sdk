package io.github.starmap.sdk;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * StarMap 客户端配置。
 */
@Getter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class StarMapClientOptions {

    /**
     * StarMap HTTP 基础地址，例如 http://127.0.0.1:8080。
     */
    private String baseUrl;

    /**
     * 单次请求超时时间。
     */
    @Builder.Default
    private Duration requestTimeout = Duration.ofSeconds(5);

    /**
     * 是否在收到 not_leader 后自动跟随 leader 地址重试写请求。
     */
    @Builder.Default
    private boolean followLeaderRedirect = true;

    /**
     * 最大 leader 跟随次数。
     */
    @Builder.Default
    private int maxLeaderRedirects = 1;

    /**
     * 默认附加请求头。
     */
    @Builder.Default
    private Map<String, String> defaultHeaders = new LinkedHashMap<>();
}
