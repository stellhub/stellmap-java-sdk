package io.github.stellmap;

import io.github.stellmap.exception.StellMapServerException;
import io.github.stellmap.exception.StellMapTransportException;
import io.github.stellmap.model.RegistryWatchEvent;
import io.github.stellmap.model.RegistryWatchRequest;
import io.github.stellmap.model.StarMapErrorResponse;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.Call;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 带本地目录缓存的受管 watch 订阅。 */
final class ManagedDirectoryWatchSubscription implements ServiceDirectorySubscription {

    private static final Logger log =
            LoggerFactory.getLogger(ManagedDirectoryWatchSubscription.class);

    private final StellMapClient owner;
    private final HttpTransport transport;
    private final RegistryWatchRequest request;
    private final RegistryWatchListener listener;
    private final OrderedCallbackDispatcher dispatcher;
    private final DefaultServiceDirectory directory = new DefaultServiceDirectory();
    private final AtomicReference<Call> callRef = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> reconnectFutureRef = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicBoolean initialConnected = new AtomicBoolean(false);
    private final CompletableFuture<ServiceDirectorySubscription> initialOpenFuture =
            new CompletableFuture<>();
    private final AtomicLong lastRevision;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong trackedServiceCount = new AtomicLong(0L);
    private final AtomicLong trackedInstanceCount = new AtomicLong(0L);

    ManagedDirectoryWatchSubscription(
            StellMapClient owner, RegistryWatchRequest request, RegistryWatchListener listener) {
        this.owner = Objects.requireNonNull(owner, "owner must not be null");
        this.transport = owner.transportInternal();
        this.request = Objects.requireNonNull(request, "request must not be null");
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.dispatcher = new OrderedCallbackDispatcher(owner.watchCallbackExecutorInternal());
        this.lastRevision = new AtomicLong(Math.max(0L, request.getSinceRevision()));
    }

    void openInitial() {
        connect(false);
        try {
            initialOpenFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StellMapTransportException("Interrupted while opening StarMap watch stream", e);
        } catch (ExecutionException e) {
            throw transport.unwrapAsTransport(e.getCause(), "Failed to open StarMap watch stream");
        }
    }

    @Override
    public ServiceDirectory getServiceDirectory() {
        return directory;
    }

    @Override
    public long getLastRevision() {
        return lastRevision.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get() || terminated.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        terminate();
    }

    void handleConnectAttemptFailed(boolean reconnectAttempt, Throwable failure) {
        if (closed.get()) {
            terminate();
            return;
        }
        if (!reconnectAttempt && !initialConnected.get()) {
            initialOpenFuture.completeExceptionally(failure);
            return;
        }
        handleReconnectableFailure(failure);
    }

    void handleStreamOpen() {
        if (closed.get()) {
            Call call = callRef.get();
            if (call != null) {
                call.cancel();
            }
            return;
        }
        cancelReconnect();
        reconnectAttempts.set(0);
        initialConnected.set(true);
        if (initialOpenFuture.complete(this)) {
            owner.clientMetrics().recordWatchSubscriptionOpened();
            log.info(
                    "Opened watch subscription namespace={}, services={}, servicePrefixes={}",
                    request.getNamespace(),
                    request.getServices(),
                    request.getServicePrefixes());
            dispatcher.dispatch(listener::onOpen);
            return;
        }
        log.debug(
                "Reconnected watch subscription namespace={}, revision={}",
                request.getNamespace(),
                effectiveSinceRevision());
        dispatcher.dispatch(listener::onOpen);
    }

    void handleEvent(RegistryWatchEvent event) {
        if (closed.get()) {
            return;
        }
        if (isResetEvent(event)) {
            lastRevision.set(0L);
        } else if (event.getRevision() > 0L) {
            lastRevision.accumulateAndGet(event.getRevision(), Math::max);
        }
        directory.apply(event);
        syncDirectoryMetrics();
        owner.clientMetrics().recordWatchEvent(event.getType());
        log.debug(
                "Applied watch event namespace={}, service={}, type={}, revision={}, directoryRevision={},"
                        + " serviceCount={}, instanceCount={}",
                event.getNamespace(),
                event.getService(),
                event.getType(),
                event.getRevision(),
                directory.getDirectoryRevision(),
                trackedServiceCount.get(),
                trackedInstanceCount.get());
        dispatcher.dispatch(() -> listener.onEvent(event));
    }

    void handleNonTerminalError(Throwable throwable) {
        if (closed.get()) {
            return;
        }
        dispatcher.dispatch(() -> listener.onError(throwable));
    }

    void handleStreamClosed(boolean reconnectAttempt, boolean streamOpened, Throwable failure) {
        if (closed.get()) {
            terminate();
            return;
        }

        Throwable actualFailure =
                failure != null
                        ? failure
                        : new StellMapTransportException(
                                streamOpened
                                        ? "StarMap watch stream disconnected"
                                        : "Connection closed before StarMap watch stream was established");

        if (!streamOpened && !initialConnected.get() && !reconnectAttempt) {
            initialOpenFuture.completeExceptionally(actualFailure);
            return;
        }
        handleReconnectableFailure(actualFailure);
    }

    StellMapClient owner() {
        return owner;
    }

    private void connect(boolean reconnectAttempt) {
        if (closed.get()) {
            return;
        }
        long sinceRevision = effectiveSinceRevision();
        boolean includeSnapshot = shouldIncludeSnapshot(sinceRevision);
        URI watchUri =
                transport.resolve(
                        owner.baseUriInternal(),
                        "/api/v1/registry/watch"
                                + owner
                                        .requestNormalizerInternal()
                                        .buildWatchQuery(request, sinceRevision, includeSnapshot));
        log.debug(
                "Opening watch stream namespace={}, services={}, servicePrefixes={}, sinceRevision={},"
                        + " includeSnapshot={}, reconnectAttempt={}",
                request.getNamespace(),
                request.getServices(),
                request.getServicePrefixes(),
                sinceRevision,
                includeSnapshot,
                reconnectAttempt);
        try {
            owner.watchExecutorInternal().execute(() -> openWatchStream(watchUri, reconnectAttempt));
        } catch (RuntimeException e) {
            handleConnectAttemptFailed(
                    reconnectAttempt,
                    new StellMapTransportException("Failed to submit StarMap watch stream task", e));
        }
    }

    private void openWatchStream(URI watchUri, boolean reconnectAttempt) {
        Request request = transport.buildWatchRequest(watchUri);
        Call call = transport.newCall(request);
        callRef.set(call);
        try (Response response = call.execute()) {
            new WatchResponseHandler(owner, this, reconnectAttempt).handle(response);
        } catch (Exception e) {
            if (!closed.get()) {
                handleConnectAttemptFailed(
                        reconnectAttempt,
                        new StellMapTransportException("Failed to open StarMap watch stream", e));
            }
        } finally {
            callRef.compareAndSet(call, null);
        }
    }

    private void handleReconnectableFailure(Throwable failure) {
        if (closed.get()) {
            terminate();
            return;
        }
        if (shouldResetRevision(failure)) {
            lastRevision.set(0L);
            directory.clear();
            syncDirectoryMetrics();
            log.warn(
                    "Resetting watch revision and directory cache namespace={} due to failure={}",
                    request.getNamespace(),
                    failure.toString());
        }
        if (!owner.watchAutoReconnectEnabled()) {
            log.warn(
                    "Watch subscription terminated without auto reconnect namespace={}, failure={}",
                    request.getNamespace(),
                    failure.toString());
            dispatcher.dispatch(() -> listener.onError(failure));
            terminate();
            return;
        }

        int attempt = reconnectAttempts.incrementAndGet();
        if (owner.watchReconnectMaxAttemptsInternal() >= 0
                && attempt > owner.watchReconnectMaxAttemptsInternal()) {
            log.error(
                    "Watch reconnect attempts exhausted namespace={}, attempts={}",
                    request.getNamespace(),
                    attempt,
                    failure);
            dispatcher.dispatch(
                    () ->
                            listener.onError(
                                    new StellMapTransportException(
                                            "StarMap watch reconnect attempts exhausted", failure)));
            terminate();
            return;
        }

        Duration delay = calculateReconnectDelay(attempt);
        owner.clientMetrics().recordWatchReconnect(attempt, failure);
        log.warn(
                "Scheduling watch reconnect namespace={}, attempt={}, delayMs={}, failure={}",
                request.getNamespace(),
                attempt,
                delay.toMillis(),
                failure.toString());
        dispatcher.dispatch(() -> listener.onError(failure));
        ScheduledFuture<?> reconnectFuture =
                owner.watchReconnectExecutorInternal()
                        .schedule(() -> connect(true), delay.toMillis(), TimeUnit.MILLISECONDS);
        replaceReconnectFuture(reconnectFuture);
    }

    private void replaceReconnectFuture(ScheduledFuture<?> reconnectFuture) {
        ScheduledFuture<?> previous = reconnectFutureRef.getAndSet(reconnectFuture);
        if (previous != null) {
            previous.cancel(false);
        }
    }

    private void cancelReconnect() {
        ScheduledFuture<?> reconnectFuture = reconnectFutureRef.getAndSet(null);
        if (reconnectFuture != null) {
            reconnectFuture.cancel(false);
        }
    }

    private Duration calculateReconnectDelay(int attempt) {
        long base = owner.watchReconnectInitialDelayInternal().toMillis();
        long max = owner.watchReconnectMaxDelayInternal().toMillis();
        long multiplier = 1L;
        for (int index = 1; index < attempt; index++) {
            if (multiplier >= Long.MAX_VALUE / 2L) {
                multiplier = Long.MAX_VALUE;
                break;
            }
            multiplier = multiplier * 2L;
        }
        long candidate;
        if (multiplier == Long.MAX_VALUE || base > Long.MAX_VALUE / Math.max(1L, multiplier)) {
            candidate = Long.MAX_VALUE;
        } else {
            candidate = base * multiplier;
        }
        return Duration.ofMillis(Math.min(candidate, max));
    }

    private long effectiveSinceRevision() {
        long revision = lastRevision.get();
        return revision > 0L ? revision : Math.max(0L, request.getSinceRevision());
    }

    private boolean shouldIncludeSnapshot(long sinceRevision) {
        return sinceRevision <= 0L && request.isIncludeSnapshot();
    }

    private boolean isResetEvent(RegistryWatchEvent event) {
        return owner.hasText(event.getType()) && "RESET".equalsIgnoreCase(event.getType().trim());
    }

    private boolean shouldResetRevision(Throwable failure) {
        if (failure instanceof StellMapServerException serverException) {
            StarMapErrorResponse errorResponse = serverException.getErrorResponse();
            if (errorResponse != null && owner.hasText(errorResponse.getCode())) {
                String code = errorResponse.getCode().trim().toLowerCase();
                return "revision_expired".equals(code)
                        || "revision_too_old".equals(code)
                        || "watch_reset".equals(code);
            }
            return serverException.getStatusCode() == 409 || serverException.getStatusCode() == 410;
        }
        return false;
    }

    private void terminate() {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }
        cancelReconnect();
        Call call = callRef.getAndSet(null);
        if (call != null) {
            call.cancel();
        }
        if (!initialOpenFuture.isDone()) {
            initialOpenFuture.completeExceptionally(
                    new StellMapTransportException("StarMap watch subscription closed"));
        }
        if (initialConnected.get()) {
            resetDirectoryMetrics();
            owner.clientMetrics().recordWatchSubscriptionClosed();
        }
        log.info(
                "Closed watch subscription namespace={}, services={}, servicePrefixes={}",
                request.getNamespace(),
                request.getServices(),
                request.getServicePrefixes());
        dispatcher.dispatch(listener::onClosed);
    }

    private void syncDirectoryMetrics() {
        DefaultServiceDirectory.DirectoryStats stats = directory.stats();
        long serviceDelta = stats.serviceCount() - trackedServiceCount.getAndSet(stats.serviceCount());
        long instanceDelta =
                stats.instanceCount() - trackedInstanceCount.getAndSet(stats.instanceCount());
        owner.clientMetrics().recordDirectoryStateDelta(serviceDelta, instanceDelta);
    }

    private void resetDirectoryMetrics() {
        long serviceCount = trackedServiceCount.getAndSet(0L);
        long instanceCount = trackedInstanceCount.getAndSet(0L);
        owner.clientMetrics().recordDirectoryStateDelta(-serviceCount, -instanceCount);
    }
}
