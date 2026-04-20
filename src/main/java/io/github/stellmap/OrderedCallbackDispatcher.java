package io.github.stellmap;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/** 顺序回调派发器，保证回调按提交顺序串行执行。 */
final class OrderedCallbackDispatcher {

    private final ExecutorService executor;
    private final AtomicReference<CompletableFuture<Void>> tail =
            new AtomicReference<>(CompletableFuture.completedFuture(null));

    OrderedCallbackDispatcher(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * 按顺序派发一个回调任务。
     *
     * @param action 回调任务
     */
    void dispatch(Runnable action) {
        tail.updateAndGet(
                previous -> previous.exceptionally(ignored -> null).thenRunAsync(action, executor));
    }
}
