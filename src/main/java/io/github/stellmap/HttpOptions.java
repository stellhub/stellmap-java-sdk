package io.github.stellmap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** watch 运行时高级配置，保留类名仅用于兼容历史版本。 */
@Getter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class HttpOptions {

    /** watch IO 线程数，0 表示使用默认虚拟线程执行器。 */
    @Builder.Default private int threads = 0;

    /** 可选外部 watch IO 执行器。 */
    private ExecutorService executor;

    /** 可选外部 watch 重连调度器。 */
    private ScheduledExecutorService scheduler;

    /** 可选自定义线程工厂。 */
    private ThreadFactory threadFactory;
}
