package io.github.starmap;

import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategyFactory;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.Executor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Netty NioEventLoopGroup 高级配置。 */
@Getter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class NettyEventLoopOptions {

    /** EventLoop 线程数，0 表示使用 Netty 默认值。 */
    @Builder.Default private int threads = 0;

    /** 可选外部执行器。 */
    private Executor executor;

    /** 可选 EventExecutor 选择器工厂。 */
    private EventExecutorChooserFactory chooserFactory;

    /** 可选 SelectorProvider。 */
    private SelectorProvider selectorProvider;

    /** 可选 select 策略工厂。 */
    private SelectStrategyFactory selectStrategyFactory;

    /** 可选拒绝策略。 */
    private RejectedExecutionHandler rejectedExecutionHandler;

    /** 可选主任务队列工厂。 */
    private EventLoopTaskQueueFactory taskQueueFactory;

    /** 可选尾任务队列工厂。 */
    private EventLoopTaskQueueFactory tailTaskQueueFactory;
}
