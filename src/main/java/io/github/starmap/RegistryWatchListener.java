package io.github.starmap;

import io.github.starmap.model.RegistryWatchEvent;

/** StarMap watch 监听器。 */
public interface RegistryWatchListener {

    /** 在 watch 连接建立后触发。 */
    default void onOpen() {}

    /**
     * 在收到 watch 事件后触发。
     *
     * @param event StarMap watch 事件
     */
    void onEvent(RegistryWatchEvent event);

    /**
     * 在 watch 循环遇到异常时触发。
     *
     * @param throwable watch 处理异常
     */
    default void onError(Throwable throwable) {}

    /** 在 watch 连接关闭后触发。 */
    default void onClosed() {}
}
