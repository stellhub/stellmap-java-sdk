package io.github.stellmap;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** StarMap 客户端生命周期管理器。 */
final class StellMapClientLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(StellMapClientLifecycleManager.class);

    private final URI baseUri;
    private final boolean autoDeregisterOnClose;
    private final StellMapHeartbeatManager heartbeatManager;
    private final HttpTransport transport;
    private final ExecutorService watchExecutor;
    private final boolean ownsWatchExecutor;
    private final ScheduledExecutorService watchReconnectExecutor;
    private final boolean ownsWatchReconnectExecutor;
    private final ExecutorService watchCallbackExecutor;
    private final boolean ownsWatchCallbackExecutor;

    StellMapClientLifecycleManager(
            URI baseUri,
            boolean autoDeregisterOnClose,
            StellMapHeartbeatManager heartbeatManager,
            HttpTransport transport,
            ExecutorService watchExecutor,
            boolean ownsWatchExecutor,
            ScheduledExecutorService watchReconnectExecutor,
            boolean ownsWatchReconnectExecutor,
            ExecutorService watchCallbackExecutor,
            boolean ownsWatchCallbackExecutor) {
        this.baseUri = baseUri;
        this.autoDeregisterOnClose = autoDeregisterOnClose;
        this.heartbeatManager = heartbeatManager;
        this.transport = transport;
        this.watchExecutor = watchExecutor;
        this.ownsWatchExecutor = ownsWatchExecutor;
        this.watchReconnectExecutor = watchReconnectExecutor;
        this.ownsWatchReconnectExecutor = ownsWatchReconnectExecutor;
        this.watchCallbackExecutor = watchCallbackExecutor;
        this.ownsWatchCallbackExecutor = ownsWatchCallbackExecutor;
    }

    /** 关闭客户端生命周期资源。 */
    void close() {
        log.info(
                "Closing StellMapClient baseUrl={}, trackedRegistrations={}, scheduledHeartbeats={}",
                baseUri,
                heartbeatManager.trackedRegistrationCount(),
                heartbeatManager.scheduledHeartbeatCount());
        RuntimeException closeFailure = heartbeatManager.shutdown(autoDeregisterOnClose);
        transport.close();
        if (ownsWatchExecutor) {
            watchExecutor.shutdownNow();
        }
        if (ownsWatchReconnectExecutor) {
            watchReconnectExecutor.shutdownNow();
        }
        if (ownsWatchCallbackExecutor) {
            watchCallbackExecutor.shutdownNow();
        }
        log.info("Closed StellMapClient baseUrl={}", baseUri);
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
