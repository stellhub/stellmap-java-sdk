package io.github.starmap;

import io.netty.channel.EventLoopGroup;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** StarMap 客户端生命周期管理器。 */
final class StarMapClientLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(StarMapClientLifecycleManager.class);

    private final URI baseUri;
    private final boolean autoDeregisterOnClose;
    private final StarMapHeartbeatManager heartbeatManager;
    private final ExecutorService watchCallbackExecutor;
    private final boolean ownsWatchCallbackExecutor;
    private final EventLoopGroup eventLoopGroup;
    private final boolean ownsEventLoopGroup;

    StarMapClientLifecycleManager(
            URI baseUri,
            boolean autoDeregisterOnClose,
            StarMapHeartbeatManager heartbeatManager,
            ExecutorService watchCallbackExecutor,
            boolean ownsWatchCallbackExecutor,
            EventLoopGroup eventLoopGroup,
            boolean ownsEventLoopGroup) {
        this.baseUri = baseUri;
        this.autoDeregisterOnClose = autoDeregisterOnClose;
        this.heartbeatManager = heartbeatManager;
        this.watchCallbackExecutor = watchCallbackExecutor;
        this.ownsWatchCallbackExecutor = ownsWatchCallbackExecutor;
        this.eventLoopGroup = eventLoopGroup;
        this.ownsEventLoopGroup = ownsEventLoopGroup;
    }

    /** 关闭客户端生命周期资源。 */
    void close() {
        log.info(
                "Closing StarMapClient baseUrl={}, trackedRegistrations={}, scheduledHeartbeats={}",
                baseUri,
                heartbeatManager.trackedRegistrationCount(),
                heartbeatManager.scheduledHeartbeatCount());
        RuntimeException closeFailure = heartbeatManager.shutdown(autoDeregisterOnClose);
        if (ownsWatchCallbackExecutor) {
            watchCallbackExecutor.shutdownNow();
        }
        if (ownsEventLoopGroup) {
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
        log.info("Closed StarMapClient baseUrl={}", baseUri);
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
