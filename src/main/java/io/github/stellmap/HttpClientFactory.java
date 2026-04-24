package io.github.stellmap;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.OkHttpClient;

/** 客户端底层资源工厂。 */
final class HttpClientFactory {

    /**
     * 创建客户端所需的全部底层资源。
     *
     * @param settings 已归一化的客户端配置
     * @return 底层资源集合
     */
    ClientResources createResources(ClientSettings settings) {
        Objects.requireNonNull(settings, "settings must not be null");

        WatchRuntimeResources watchRuntimeResources =
                createWatchRuntimeResources(settings.httpOptions());

        ExecutorService watchCallbackExecutor =
                settings.watchCallbackExecutor() != null
                        ? settings.watchCallbackExecutor()
                        : Executors.newThreadPerTaskExecutor(
                                Thread.ofVirtual().name("starmap-watch-callback-", 0).factory());
        boolean ownsWatchCallbackExecutor = settings.watchCallbackExecutor() == null;

        ScheduledExecutorService heartbeatExecutor =
                settings.heartbeatExecutor() != null
                        ? settings.heartbeatExecutor()
                        : Executors.newSingleThreadScheduledExecutor(
                                Thread.ofVirtual().name("starmap-heartbeat-", 0).factory());
        boolean ownsHeartbeatExecutor = settings.heartbeatExecutor() == null;

        OkHttpClient okHttpClient = buildOkHttpClient(settings.requestTimeout());
        HttpTransport httpTransport =
                new HttpTransport(
                        StellMapClient.OBJECT_MAPPER,
                        okHttpClient,
                        settings.defaultHeaders(),
                        settings.metrics(),
                        settings.followLeaderRedirect());
        StellMapHeartbeatManager heartbeatManager =
                new StellMapHeartbeatManager(
                        settings.baseUri(),
                        httpTransport,
                        heartbeatExecutor,
                        ownsHeartbeatExecutor,
                        settings.metrics(),
                        settings.maxLeaderRedirects());
        StellMapClientLifecycleManager lifecycleManager =
                new StellMapClientLifecycleManager(
                        settings.baseUri(),
                        settings.autoDeregisterOnClose(),
                        heartbeatManager,
                        httpTransport,
                        watchRuntimeResources.watchExecutor(),
                        watchRuntimeResources.ownsWatchExecutor(),
                        watchRuntimeResources.watchReconnectExecutor(),
                        watchRuntimeResources.ownsWatchReconnectExecutor(),
                        watchCallbackExecutor,
                        ownsWatchCallbackExecutor);

        return new ClientResources(
                watchRuntimeResources.watchExecutor(),
                watchRuntimeResources.watchReconnectExecutor(),
                watchCallbackExecutor,
                httpTransport,
                heartbeatManager,
                lifecycleManager);
    }

    private WatchRuntimeResources createWatchRuntimeResources(HttpOptions options) {
        ExecutorService watchExecutor;
        boolean ownsWatchExecutor;
        if (options != null && options.getExecutor() != null) {
            watchExecutor = options.getExecutor();
            ownsWatchExecutor = false;
        } else if (options != null && options.getThreads() > 0) {
            watchExecutor =
                    Executors.newFixedThreadPool(
                            options.getThreads(), resolveThreadFactory(options, "starmap-watch-io-"));
            ownsWatchExecutor = true;
        } else {
            watchExecutor =
                    Executors.newThreadPerTaskExecutor(
                            Thread.ofVirtual().name("starmap-watch-io-", 0).factory());
            ownsWatchExecutor = true;
        }

        ScheduledExecutorService watchReconnectExecutor;
        boolean ownsWatchReconnectExecutor;
        if (options != null && options.getScheduler() != null) {
            watchReconnectExecutor = options.getScheduler();
            ownsWatchReconnectExecutor = false;
        } else {
            watchReconnectExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            resolveThreadFactory(options, "starmap-watch-reconnect-"));
            ownsWatchReconnectExecutor = true;
        }

        return new WatchRuntimeResources(
                watchExecutor,
                ownsWatchExecutor,
                watchReconnectExecutor,
                ownsWatchReconnectExecutor);
    }

    private ThreadFactory resolveThreadFactory(HttpOptions options, String prefix) {
        if (options != null && options.getThreadFactory() != null) {
            return options.getThreadFactory();
        }
        return new NamedThreadFactory(prefix);
    }

    private OkHttpClient buildOkHttpClient(Duration requestTimeout) {
        return new OkHttpClient.Builder()
                .connectTimeout(requestTimeout)
                .readTimeout(requestTimeout)
                .writeTimeout(requestTimeout)
                .callTimeout(requestTimeout)
                .retryOnConnectionFailure(false)
                .followRedirects(false)
                .followSslRedirects(false)
                .build();
    }

    /** 工厂输入配置。 */
    record ClientSettings(
            URI baseUri,
            Duration requestTimeout,
            boolean followLeaderRedirect,
            int maxLeaderRedirects,
            boolean autoDeregisterOnClose,
            Map<String, String> defaultHeaders,
            ExecutorService watchCallbackExecutor,
            ScheduledExecutorService heartbeatExecutor,
            HttpOptions httpOptions,
            StarMapClientMetrics metrics) {}

    /** 工厂输出资源。 */
    record ClientResources(
            ExecutorService watchExecutor,
            ScheduledExecutorService watchReconnectExecutor,
            ExecutorService watchCallbackExecutor,
            HttpTransport httpTransport,
            StellMapHeartbeatManager heartbeatManager,
            StellMapClientLifecycleManager lifecycleManager) {}

    /** watch 运行时资源。 */
    private record WatchRuntimeResources(
            ExecutorService watchExecutor,
            boolean ownsWatchExecutor,
            ScheduledExecutorService watchReconnectExecutor,
            boolean ownsWatchReconnectExecutor) {}

    /** 命名线程工厂。 */
    private static final class NamedThreadFactory implements ThreadFactory {

        private final String prefix;
        private final AtomicInteger sequence = new AtomicInteger(0);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName(prefix + sequence.incrementAndGet());
            return thread;
        }
    }
}
