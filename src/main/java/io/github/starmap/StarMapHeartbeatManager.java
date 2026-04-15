package io.github.starmap;

import com.fasterxml.jackson.databind.JavaType;
import io.github.starmap.model.DeregisterRequest;
import io.github.starmap.model.HeartbeatRequest;
import io.github.starmap.model.RegisterRequest;
import io.github.starmap.model.StarMapResponse;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 注册跟踪与定时心跳管理器。 */
final class StarMapHeartbeatManager {

    private static final Logger log = LoggerFactory.getLogger(StarMapHeartbeatManager.class);
    private static final JavaType VOID_RESPONSE_TYPE =
            StarMapClient.OBJECT_MAPPER
                    .getTypeFactory()
                    .constructParametricType(StarMapResponse.class, Void.class);

    private final URI baseUri;
    private final NettyHttpTransport transport;
    private final ScheduledExecutorService heartbeatExecutor;
    private final boolean ownsHeartbeatExecutor;
    private final StarMapClientMetrics metrics;
    private final int maxLeaderRedirects;
    private final ConcurrentMap<RegistrationKey, DeregisterRequest> registeredInstances =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<RegistrationKey, ScheduledHeartbeatSubscription>
            heartbeatSubscriptions = new ConcurrentHashMap<>();

    StarMapHeartbeatManager(
            URI baseUri,
            NettyHttpTransport transport,
            ScheduledExecutorService heartbeatExecutor,
            boolean ownsHeartbeatExecutor,
            StarMapClientMetrics metrics,
            int maxLeaderRedirects) {
        this.baseUri = baseUri;
        this.transport = transport;
        this.heartbeatExecutor = heartbeatExecutor;
        this.ownsHeartbeatExecutor = ownsHeartbeatExecutor;
        this.metrics = metrics;
        this.maxLeaderRedirects = maxLeaderRedirects;
    }

    /**
     * 记录注册成功的实例。
     *
     * @param request 已归一化的注册请求
     */
    void trackRegistration(RegisterRequest request) {
        registeredInstances.put(
                RegistrationKey.from(request),
                DeregisterRequest.builder()
                        .namespace(request.getNamespace())
                        .service(request.getService())
                        .organization(request.getOrganization())
                        .businessDomain(request.getBusinessDomain())
                        .capabilityDomain(request.getCapabilityDomain())
                        .application(request.getApplication())
                        .role(request.getRole())
                        .instanceId(request.getInstanceId())
                        .build());
        metrics.recordRegistration(true, true);
    }

    /**
     * 处理注销成功后的本地状态。
     *
     * @param request 已归一化的注销请求
     */
    void handleDeregistered(DeregisterRequest request) {
        registeredInstances.remove(RegistrationKey.from(request));
        ScheduledHeartbeatSubscription heartbeatSubscription =
                heartbeatSubscriptions.remove(RegistrationKey.from(request));
        if (heartbeatSubscription != null) {
            heartbeatSubscription.close();
        }
        metrics.recordRegistration(false, true);
    }

    /**
     * 发送一次心跳请求。
     *
     * @param request 已归一化的心跳请求
     * @return 服务端响应
     */
    StarMapResponse<Void> heartbeat(HeartbeatRequest request) {
        return executeHeartbeat(request, false);
    }

    /**
     * 启动定时心跳。
     *
     * @param request 已归一化的心跳请求
     * @param interval 心跳间隔
     * @return 心跳订阅句柄
     */
    HeartbeatSubscription scheduleHeartbeat(HeartbeatRequest request, Duration interval) {
        RegistrationKey key = RegistrationKey.from(request);
        ScheduledHeartbeatSubscription subscription =
                new ScheduledHeartbeatSubscription(request, interval);
        ScheduledHeartbeatSubscription previous = heartbeatSubscriptions.put(key, subscription);
        if (previous != null) {
            log.info(
                    "Replacing scheduled heartbeat namespace={}, service={}, instanceId={}",
                    request.getNamespace(),
                    request.getService(),
                    request.getInstanceId());
            previous.close();
        }

        ScheduledFuture<?> future =
                heartbeatExecutor.scheduleWithFixedDelay(
                        subscription::tick, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        subscription.bind(future);
        metrics.recordScheduledHeartbeatOpened();
        log.info(
                "Started scheduled heartbeat namespace={}, service={}, instanceId={}, intervalMs={}",
                request.getNamespace(),
                request.getService(),
                request.getInstanceId(),
                interval.toMillis());
        return subscription;
    }

    /**
     * 返回当前跟踪的注册实例数量。
     *
     * @return 注册实例数量
     */
    int trackedRegistrationCount() {
        return registeredInstances.size();
    }

    /**
     * 返回当前定时心跳数量。
     *
     * @return 定时心跳数量
     */
    int scheduledHeartbeatCount() {
        return heartbeatSubscriptions.size();
    }

    /**
     * 关闭心跳与注册跟踪状态。
     *
     * @param autoDeregisterOnClose 关闭时是否自动注销
     * @return 关闭过程中的异常
     */
    RuntimeException shutdown(boolean autoDeregisterOnClose) {
        closeHeartbeatSubscriptions();
        RuntimeException failure = null;
        if (autoDeregisterOnClose) {
            failure = deregisterTrackedInstancesOnClose();
        }
        if (ownsHeartbeatExecutor) {
            heartbeatExecutor.shutdownNow();
        }
        return failure;
    }

    private StarMapResponse<Void> executeHeartbeat(HeartbeatRequest request, boolean scheduled) {
        long startNanos = System.nanoTime();
        try {
            StarMapResponse<Void> response =
                    transport.executeJson(
                            "POST",
                            baseUri,
                            "/api/v1/registry/heartbeat",
                            request,
                            VOID_RESPONSE_TYPE,
                            true,
                            maxLeaderRedirects);
            metrics.recordHeartbeat(System.nanoTime() - startNanos, true, scheduled);
            return response;
        } catch (RuntimeException e) {
            metrics.recordHeartbeat(System.nanoTime() - startNanos, false, scheduled);
            throw e;
        }
    }

    private void closeHeartbeatSubscriptions() {
        List<ScheduledHeartbeatSubscription> subscriptions =
                new ArrayList<>(heartbeatSubscriptions.values());
        if (!subscriptions.isEmpty()) {
            log.info("Closing scheduled heartbeats count={}", subscriptions.size());
        }
        for (ScheduledHeartbeatSubscription subscription : subscriptions) {
            subscription.close();
        }
    }

    private RuntimeException deregisterTrackedInstancesOnClose() {
        RuntimeException failure = null;
        List<DeregisterRequest> pending = new ArrayList<>(registeredInstances.values());
        int successCount = 0;
        for (DeregisterRequest request : pending) {
            try {
                transport.executeJson(
                        "POST",
                        baseUri,
                        "/api/v1/registry/deregister",
                        request,
                        VOID_RESPONSE_TYPE,
                        true,
                        maxLeaderRedirects);
                registeredInstances.remove(RegistrationKey.from(request));
                metrics.recordAutoDeregister(true);
                metrics.recordRegistration(false, true);
                successCount++;
                log.info(
                        "Auto deregistered instance namespace={}, service={}, instanceId={}",
                        request.getNamespace(),
                        request.getService(),
                        request.getInstanceId());
            } catch (RuntimeException e) {
                metrics.recordAutoDeregister(false);
                log.warn(
                        "Failed to auto deregister instance namespace={}, service={}, instanceId={}",
                        request.getNamespace(),
                        request.getService(),
                        request.getInstanceId(),
                        e);
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        log.info(
                "Finished auto deregister on close total={}, success={}, failed={}",
                pending.size(),
                successCount,
                pending.size() - successCount);
        return failure;
    }

    private boolean isInterruptedFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private final class ScheduledHeartbeatSubscription implements HeartbeatSubscription {

        private final HeartbeatRequest request;
        private final Duration interval;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

        private ScheduledHeartbeatSubscription(HeartbeatRequest request, Duration interval) {
            this.request = request;
            this.interval = interval;
        }

        @Override
        public boolean isClosed() {
            ScheduledFuture<?> future = futureRef.get();
            return closed.get() || (future != null && future.isCancelled());
        }

        @Override
        public HeartbeatRequest getRequest() {
            return request;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            heartbeatSubscriptions.remove(RegistrationKey.from(request), this);
            ScheduledFuture<?> future = futureRef.getAndSet(null);
            if (future != null) {
                future.cancel(false);
            }
            metrics.recordScheduledHeartbeatClosed();
            log.info(
                    "Stopped scheduled heartbeat namespace={}, service={}, instanceId={}, intervalMs={}",
                    request.getNamespace(),
                    request.getService(),
                    request.getInstanceId(),
                    interval.toMillis());
        }

        private void bind(ScheduledFuture<?> future) {
            futureRef.set(future);
        }

        private void tick() {
            if (closed.get()) {
                return;
            }
            try {
                executeHeartbeat(request, true);
                log.debug(
                        "Scheduled heartbeat succeeded namespace={}, service={}, instanceId={}",
                        request.getNamespace(),
                        request.getService(),
                        request.getInstanceId());
            } catch (RuntimeException e) {
                if (closed.get() && isInterruptedFailure(e)) {
                    log.debug(
                            "Scheduled heartbeat interrupted during shutdown namespace={}, service={},"
                                    + " instanceId={}",
                            request.getNamespace(),
                            request.getService(),
                            request.getInstanceId(),
                            e);
                    return;
                }
                log.warn(
                        "Scheduled heartbeat failed namespace={}, service={}, instanceId={}",
                        request.getNamespace(),
                        request.getService(),
                        request.getInstanceId(),
                        e);
            }
        }
    }
}
