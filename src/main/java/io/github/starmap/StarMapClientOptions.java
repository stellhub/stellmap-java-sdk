package io.github.starmap;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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
     * 是否在 close 时自动注销当前客户端成功注册过的实例。
     */
    @Builder.Default
    private boolean autoDeregisterOnClose = false;

    /**
     * watch 连接断开后是否自动重连。
     */
    @Builder.Default
    private boolean watchAutoReconnect = true;

    /**
     * watch 首次重连延迟。
     */
    @Builder.Default
    private Duration watchReconnectInitialDelay = Duration.ofSeconds(1);

    /**
     * watch 最大重连延迟。
     */
    @Builder.Default
    private Duration watchReconnectMaxDelay = Duration.ofSeconds(10);

    /**
     * watch 最大重连次数，-1 表示不限。
     */
    @Builder.Default
    private int watchReconnectMaxAttempts = -1;

    /**
     * 可选外部 watch 回调线程池。
     */
    private ExecutorService watchCallbackExecutor;

    /**
     * 可选外部心跳定时线程池。
     */
    private ScheduledExecutorService heartbeatExecutor;

    /**
     * 可选 Netty EventLoopGroup 高级配置。
     */
    private NettyEventLoopOptions nettyEventLoopOptions;

    /**
     * 默认附加请求头。
     */
    @Builder.Default
    private Map<String, String> defaultHeaders = new LinkedHashMap<>();
}
