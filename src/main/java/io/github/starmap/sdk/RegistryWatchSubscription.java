package io.github.starmap.sdk;

/**
 * StarMap watch 订阅句柄。
 */
public interface RegistryWatchSubscription extends AutoCloseable {

    /**
     * 返回 watch 是否已经关闭。
     *
     * @return true 表示已经关闭
     */
    boolean isClosed();

    /**
     * 主动关闭 watch 订阅。
     */
    @Override
    void close();
}
