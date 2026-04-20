package io.github.stellmap;

import io.github.stellmap.model.HeartbeatRequest;

/** StarMap 定时心跳句柄。 */
public interface HeartbeatSubscription extends AutoCloseable {

    /**
     * 返回当前心跳是否已经关闭。
     *
     * @return true 表示已关闭
     */
    boolean isClosed();

    /**
     * 返回心跳请求。
     *
     * @return 心跳请求
     */
    HeartbeatRequest getRequest();

    /** 主动关闭定时心跳。 */
    @Override
    void close();
}
