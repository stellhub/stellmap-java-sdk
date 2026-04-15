package io.github.starmap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.internal.PlatformDependent;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.nio.channels.spi.SelectorProvider;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Netty 客户端底层资源工厂。
 */
final class NettyClientFactory {

    /**
     * 创建客户端所需的全部底层资源。
     *
     * @param settings 已归一化的客户端配置
     * @return 底层资源集合
     */
    ClientResources createResources(ClientSettings settings) {
        Objects.requireNonNull(settings, "settings must not be null");

        EventLoopGroup eventLoopGroup = settings.eventLoopGroupOverride() != null
                ? settings.eventLoopGroupOverride()
                : createEventLoopGroup(settings.nettyEventLoopOptions());
        boolean ownsEventLoopGroup = settings.eventLoopGroupOverride() == null;

        Bootstrap bootstrapTemplate = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis(settings.requestTimeout()));

        ExecutorService watchCallbackExecutor = settings.watchCallbackExecutor() != null
                ? settings.watchCallbackExecutor()
                : Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("starmap-watch-callback-", 0).factory());
        boolean ownsWatchCallbackExecutor = settings.watchCallbackExecutor() == null;

        ScheduledExecutorService heartbeatExecutor = settings.heartbeatExecutor() != null
                ? settings.heartbeatExecutor()
                : Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("starmap-heartbeat-", 0).factory());
        boolean ownsHeartbeatExecutor = settings.heartbeatExecutor() == null;

        SslContext sslContext = buildSslContext();
        NettyHttpTransport httpTransport = new NettyHttpTransport(
                StarMapClient.OBJECT_MAPPER,
                eventLoopGroup,
                bootstrapTemplate,
                settings.requestTimeout(),
                settings.defaultHeaders(),
                sslContext,
                settings.metrics(),
                settings.followLeaderRedirect()
        );
        StarMapHeartbeatManager heartbeatManager = new StarMapHeartbeatManager(
                settings.baseUri(),
                httpTransport,
                heartbeatExecutor,
                ownsHeartbeatExecutor,
                settings.metrics(),
                settings.maxLeaderRedirects()
        );
        StarMapClientLifecycleManager lifecycleManager = new StarMapClientLifecycleManager(
                settings.baseUri(),
                settings.autoDeregisterOnClose(),
                heartbeatManager,
                watchCallbackExecutor,
                ownsWatchCallbackExecutor,
                eventLoopGroup,
                ownsEventLoopGroup
        );

        return new ClientResources(
                eventLoopGroup,
                ownsEventLoopGroup,
                bootstrapTemplate,
                watchCallbackExecutor,
                ownsWatchCallbackExecutor,
                sslContext,
                httpTransport,
                heartbeatManager,
                lifecycleManager
        );
    }

    private EventLoopGroup createEventLoopGroup(NettyEventLoopOptions options) {
        if (options == null) {
            return new NioEventLoopGroup();
        }
        int threads = Math.max(0, options.getThreads());
        ExecutorService executor = options.getExecutor() instanceof ExecutorService executorService ? executorService : null;
        EventExecutorChooserFactory chooserFactory = options.getChooserFactory() != null
                ? options.getChooserFactory()
                : DefaultEventExecutorChooserFactory.INSTANCE;
        SelectorProvider selectorProvider = options.getSelectorProvider() != null
                ? options.getSelectorProvider()
                : SelectorProvider.provider();
        io.netty.channel.SelectStrategyFactory selectStrategyFactory = options.getSelectStrategyFactory() != null
                ? options.getSelectStrategyFactory()
                : DefaultSelectStrategyFactory.INSTANCE;
        RejectedExecutionHandler rejectedExecutionHandler = options.getRejectedExecutionHandler() != null
                ? options.getRejectedExecutionHandler()
                : RejectedExecutionHandlers.reject();
        EventLoopTaskQueueFactory taskQueueFactory = options.getTaskQueueFactory() != null
                ? options.getTaskQueueFactory()
                : PlatformDependent::newMpscQueue;
        EventLoopTaskQueueFactory tailTaskQueueFactory = options.getTailTaskQueueFactory() != null
                ? options.getTailTaskQueueFactory()
                : PlatformDependent::newMpscQueue;

        if (executor == null
                && options.getChooserFactory() == null
                && options.getSelectorProvider() == null
                && options.getSelectStrategyFactory() == null
                && options.getRejectedExecutionHandler() == null
                && options.getTaskQueueFactory() == null
                && options.getTailTaskQueueFactory() == null) {
            return new NioEventLoopGroup(threads);
        }
        return new NioEventLoopGroup(
                threads,
                options.getExecutor(),
                chooserFactory,
                selectorProvider,
                selectStrategyFactory,
                rejectedExecutionHandler,
                taskQueueFactory,
                tailTaskQueueFactory
        );
    }

    private int connectTimeoutMillis(Duration requestTimeout) {
        long timeoutMillis = requestTimeout.toMillis();
        return timeoutMillis > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutMillis;
    }

    private SslContext buildSslContext() {
        try {
            return SslContextBuilder.forClient().build();
        } catch (SSLException e) {
            throw new IllegalStateException("Failed to initialize Netty SSL context", e);
        }
    }

    /**
     * 工厂输入配置。
     */
    record ClientSettings(
            URI baseUri,
            Duration requestTimeout,
            boolean followLeaderRedirect,
            int maxLeaderRedirects,
            boolean autoDeregisterOnClose,
            Map<String, String> defaultHeaders,
            EventLoopGroup eventLoopGroupOverride,
            ExecutorService watchCallbackExecutor,
            ScheduledExecutorService heartbeatExecutor,
            NettyEventLoopOptions nettyEventLoopOptions,
            StarMapClientMetrics metrics
    ) {
    }

    /**
     * 工厂输出资源。
     */
    record ClientResources(
            EventLoopGroup eventLoopGroup,
            boolean ownsEventLoopGroup,
            Bootstrap bootstrapTemplate,
            ExecutorService watchCallbackExecutor,
            boolean ownsWatchCallbackExecutor,
            SslContext sslContext,
            NettyHttpTransport httpTransport,
            StarMapHeartbeatManager heartbeatManager,
            StarMapClientLifecycleManager lifecycleManager
    ) {
    }
}
