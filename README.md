# StellMap Java SDK

StellMap Java SDK 用于服务注册与服务发现。

这个 SDK 的设计目标不是单纯发几个 HTTP 请求，而是为业务服务、网关、边车、控制面组件提供一套稳定的注册、发现与变更感知能力。在小规模场景下，注册与查询只需要同步请求；在大规模场景下，尤其是网关一次关注成千上万个下游服务时，SDK 必须具备更强的长连接管理、事件流处理和后续本地缓存扩展能力。

## 1. SDK 最终设计目标

StellMap Java SDK 的最终目标如下：

1. 提供稳定的服务注册、注销、心跳与实例查询能力。
2. 提供面向大规模服务发现的 watch 能力，而不是只面向单个服务的简单长连接订阅。
3. 支持事件驱动的目录变更感知，让 SDK 可以在注册中心节点变更时主动推送并回调业务侧。
4. 支持 SDK 内部维护本地服务目录缓存，让调用方优先读取本地快照而不是频繁回源。
5. 支持断线重连、基于 revision 的续传与事件补偿，保证 watch 的可恢复性。
6. 为网关、Service Mesh、任务调度器、配置分发器等高并发场景提供更可扩展的网络传输层。

一句话总结：**这个 SDK 最终要成为一个“可恢复、可缓存、可聚合、可扩展”的服务目录客户端。**

## 2. 为什么当前实现切换到 OkHttp

当前版本已经将同步请求与 watch/SSE 都统一到 OkHttp。

切换原因不是因为 Netty 不能胜任，而是因为当前 SDK 更需要：

1. 用更小的依赖面提供稳定的注册、注销、心跳、查询能力。
2. 继续保留 watch 的长连接、自动重连、revision 续传与本地缓存能力。
3. 让普通请求和 SSE 订阅都复用同一套 HTTP 客户端与连接池。
4. 降低 SDK 对外暴露的底层网络栈概念，减少业务方理解和接入成本。

当前实现的结论是：**对于这个版本的 StellMap SDK，OkHttp 足以承载普通请求与 SSE watch，并且可以让整体依赖和资源模型更简单。**

## 3. 当前实现与目标设计

### 当前已经实现

1. 注册、注销、心跳、查询已经基于 OkHttp 发送。
2. `watchInstances` 和 `watchDirectory` 都已经通过 OkHttp 长连接读取 `text/event-stream` 事件流。
3. SDK 已经具备事件回调接口：
   - `onOpen()`
   - `onEvent(RegistryWatchEvent event)`
   - `onError(Throwable throwable)`
   - `onClosed()`
4. watch 流已经支持从 SSE 中解析 `event`、`id`、`data`，并将 `id` 映射为 `revision`。
5. 目录订阅已经支持自动重连、指数退避、基于 `sinceRevision` 的 revision 续传。
6. 目录订阅已经内置本地服务缓存，业务方可以直接读取 `ServiceDirectory`。
7. 客户端已经支持可选的 `autoDeregisterOnClose`、可注入的 watch 回调线程池、可注入的心跳调度线程池、可注入的 OpenTelemetry 指标入口以及可配置的 watch 运行时参数。
8. 客户端已经支持 `scheduleHeartbeat(...)` 与 `registerAndScheduleHeartbeat(...)`，方便服务提供方托管定时心跳。

### 后续仍可继续增强

1. 更细粒度的重连观测指标与监控埋点
2. 更明确的事件类型枚举与 schema 约束
3. 针对超大目录的缓存淘汰与分片策略
4. 更高层的负载均衡与路由集成 API

## 4. 事件结构怎么定义

当前 `RegistryWatchEvent` 定义如下：

```java
public class RegistryWatchEvent {
    private long revision;
    private String type;
    private String namespace;
    private String service;
    private String organization;
    private String businessDomain;
    private String capabilityDomain;
    private String application;
    private String role;
    private String instanceId;
    private RegistryInstance instance;
    private List<RegistryInstance> instances;
}
```

这个结构的设计意图是同时兼容：

1. 单实例增量事件
2. 单服务全量快照事件
3. 后续聚合目录事件

推荐的事件语义如下：

| 字段 | 含义 |
| --- | --- |
| `revision` | 目录版本号，用于断线续传与幂等处理 |
| `type` | 事件类型，例如 `SNAPSHOT`、`UPSERT`、`DELETE`、`RESET` |
| `namespace` | 命名空间 |
| `service` | 规范化服务名，例如 `company.trade.order.order-center.api` |
| `organization` | 组织标识 |
| `businessDomain` | 业务域 |
| `capabilityDomain` | 能力域 |
| `application` | 应用名 |
| `role` | 应用角色 |
| `instanceId` | 实例标识 |
| `instance` | 单实例增量内容 |
| `instances` | 当前服务的全量实例快照 |

推荐事件类型约束：

1. `SNAPSHOT`
   当前服务或当前目录的一次全量视图，通常用于首次建缓存。
2. `UPSERT`
   某个实例新增或更新。
3. `DELETE`
   某个实例被移除。
4. `RESET`
   表示客户端 revision 已过期，应该重新拉取快照并重建缓存。

推荐事件示例：

```json
{
  "revision": 1024,
  "type": "UPSERT",
  "namespace": "prod",
  "service": "company.trade.order.order-center.api",
  "organization": "company",
  "businessDomain": "trade",
  "capabilityDomain": "order",
  "application": "order-center",
  "role": "api",
  "instanceId": "order-center-api-10.0.0.12-8080",
  "instance": {
    "namespace": "prod",
    "service": "company.trade.order.order-center.api",
    "instanceId": "order-center-api-10.0.0.12-8080"
  }
}
```

## 5. 本地缓存怎么组织

对于服务发现 SDK，业务方最终最关心的不是“我收到了多少事件”，而是“我现在有哪些可用实例”。

因此本地缓存建议组织为：

```java
Map<ServiceKey, ServiceSnapshot>
```

其中：

```java
record ServiceKey(String namespace, String service) {}
```

这里的 `service` 始终使用规范化完整服务名，结构化字段则随实例一起保留，便于业务方按组织、业务域、能力域做日志、监控和路由聚合。

```java
class ServiceSnapshot {
    long revision;
    Map<String, RegistryInstance> instancesById;
}
```

设计原因如下：

1. `ServiceKey` 作为一级索引，适合网关、负载均衡器按服务读取实例列表。
2. `instancesById` 作为二级索引，适合处理 `UPSERT` 与 `DELETE` 增量事件。
3. 每个服务保存自己的最新 `revision`，便于局部校验和调试。
4. 如果后续支持目录级聚合订阅，也可以再加一层全局 `directoryRevision`。

推荐读取模式：

1. SDK 通过 watch 持续更新本地缓存。
2. 调用方查询某个服务实例列表时，优先读取本地缓存。
3. 只有在首次冷启动或缓存失效时才回源做一次快照拉取。

这样可以显著减少对注册中心的查询压力。

## 6. 自动重连与 revision 应该怎么做

当前版本已经实现了可恢复 watch：

1. 首次 watch 建立成功后，SDK 会保存最近一次成功处理的 `revision`。
2. 连接中断后，SDK 会按指数退避策略自动重连，默认从 `1s` 开始，最大退避到 `10s`。
3. 重连时会携带最近 revision，并通过查询参数写入 `sinceRevision`。
4. `sinceRevision` 表示“客户端已经成功消费到的最后一个目录事件版本号”，它的作用是恢复 watch 事件流，而不是做大文件断点续传。
5. 服务端若判断 revision 仍然有效，则只返回断线期间的增量事件。
6. 服务端若判断 revision 已超出历史保留窗口：
   - `includeSnapshot=true` 时，可以退回到全量 `snapshot`
   - `includeSnapshot=false` 时，应返回 `revision_expired`
7. SDK 在收到 `revision_expired`、`revision_too_old`、`watch_reset` 或 `409/410` 后，会重置本地 revision 并清空目录缓存，随后重新建立订阅。

推荐状态流转：

1. `SNAPSHOT`
2. 连续 `UPSERT / DELETE`
3. 连接断开
4. 自动重连
5. 基于 revision 续传
6. 如续传失败则全量重建

为什么必须这样设计：

1. watch 长连接在生产环境中一定会断。
2. 如果没有 revision，断线期间的事件会丢失。
3. 如果没有自动重连，业务调用方必须自己处理连接恢复，SDK 就失去了基础设施封装价值。

## 7. watch API 要怎么设计以及为什么要这样设计

### 当前 API

当前 SDK 同时暴露两层 watch API：

```java
RegistryWatchSubscription watchInstances(
    RegistryQueryRequest request,
    RegistryWatchListener listener
)
```

```java
ServiceDirectorySubscription watchDirectory(
    RegistryWatchRequest request,
    RegistryWatchListener listener
)
```

其中 `RegistryWatchRequest` 当前已经支持：

1. `namespace`
2. `service`
3. `services`
4. `servicePrefixes`
5. `organization`
6. `businessDomain`
7. `capabilityDomain`
8. `application`
9. `role`
10. `zone`
11. `endpoint`
12. `selectors`
13. `labels`
14. `sinceRevision`
15. `includeSnapshot`

多层级服务标识的使用约定：

1. `service` 是规范化完整服务名，例如 `company.trade.order.order-center.api`
2. 如果只填写 `organization -> role` 五段结构化字段，SDK 会自动组合出 `service`
3. 如果 `service` 和结构化字段同时填写，它们必须一致
4. 结构化字段用于 query/watch 时必须连续，不能跳层；例如只填 `organization + businessDomain + capabilityDomain` 时，SDK 会把它转成一个 `servicePrefix=company.trade.order`

`servicePrefix` 语义约定：

1. 它匹配规范化服务名的前缀
2. `servicePrefix=company.trade.order` 可以匹配：
   - `company.trade.order.order-center.api`
   - `company.trade.order.order-center.worker`
3. 它适合网关、服务治理组件一次订阅一个业务域或能力域下的目录

为什么要这样设计：

1. 网关关注的是一个服务目录，而不是某一个服务的单点变化。
2. 一次 watch 上万个服务时，开上万条连接是不合理的。
3. 更好的模型是少量聚合连接 + 事件内带 `serviceKey`。
4. `watchDirectory` 返回 `ServiceDirectorySubscription`，说明 watch 不只是“收事件”，还会同步维护本地目录视图。
5. `watchInstances` 则保留成面向单服务的便捷入口，本质上是对聚合 watch 的包装。

换句话说，watch API 的目标不是“监听一个服务”，而是“订阅一个目录范围，并稳定维护它的本地视图”。

## 8. 为什么事件回调仍然是合理的

当前 `RegistryWatchListener` 仍然保留回调接口，这个方向是合理的。

原因如下：

1. 注册中心本质上就是事件源。
2. watch 的本质就是服务端主动推送，客户端异步消费。
3. 回调接口能很好地表达连接建立、事件到达、异常、关闭这些生命周期。
4. 后续即使增加本地缓存，底层仍然是由事件回调驱动缓存更新。

因此，推荐的分层方式是：

1. 低层接口保留事件监听器 `RegistryWatchListener`
2. 高层能力新增目录缓存接口，例如 `ServiceDirectory`
3. 业务方既可以直接监听事件，也可以只读取 SDK 已维护好的本地视图

## 9. 一个更符合长期目标的架构

推荐架构分层如下：

1. `StellMapClient`
   负责注册、注销、心跳、查询和底层 watch 连接管理。
2. `ManagedDirectoryWatchSubscription`
   负责自动重连、revision 续传、订阅恢复。
3. `DefaultServiceDirectory`
   负责消费 watch 事件并维护本地目录缓存。
4. `ServiceDirectory`
   对业务暴露按服务读取实例列表、按 namespace 查询目录等只读接口。

这样分层的好处：

1. 传输层与目录语义解耦。
2. 普通业务调用只需要读缓存，不需要感知网络细节。
3. 后续如果切换协议或继续优化底层 HTTP 实现，不会影响上层目录 API。

## 10. 资源与指标配置

当前版本的 `StellMapClient` 还支持以下资源和指标控制能力：

1. `autoDeregisterOnClose`
   允许在 `close()` 时对当前客户端成功注册过的实例执行自动注销。
   这个能力默认关闭，因为 `StellMapClient` 也可能只是纯发现客户端。
2. `watchCallbackExecutor`
   允许业务方传入自己的 watch 回调线程池，便于统一线程池治理和指标采集。
3. `OpenTelemetry`
   `StellMapClient` 构造器支持显式传入 `OpenTelemetry`，用于构建请求量、请求时延、心跳量、心跳时延、watch 事件量、watch 重连次数、活动订阅数、本地目录缓存规模和自动注销等指标。
4. `NettyEventLoopOptions`
   该配置类出于兼容性保留原名称，但现在用于配置 watch IO 执行器、重连调度器与线程工厂。

这些能力的设计目标是：**把 StellMapClient 从“可用”提升到“可运营、可观测、可治理”。**

## 11. 结论

StellMap Java SDK 的设计目的，是让服务注册与服务发现不仅“可调用”，更要“可持续运行”。

因此本项目的选型与设计基线如下：

1. 注册与查询是基础能力，但 watch 才是服务发现的核心能力。
2. 在当前实现里，OkHttp 既承载同步请求，也承载 watch/SSE 长连接。
3. watch 不应该停留在“一服务一连接”的简单模型，而应该演进为“聚合目录订阅 + 事件驱动缓存”。
4. revision、自动重连、本地缓存和聚合 watch，是这个 SDK 必须补齐的核心能力。
5. 这个 SDK 的最终形态，应该是一个面向生产环境的大规模服务目录客户端，而不是简单的 REST 包装器。

## 12. 当前使用方式

当前版本既支持单服务 watch，也支持聚合目录 watch。

单服务 watch：

```java
StellMapClient client = new StellMapClient(
        StellMapClientOptions.builder()
                .baseUrl("http://127.0.0.1:8080")
                .watchAutoReconnect(true)
                .watchReconnectInitialDelay(Duration.ofSeconds(1))
                .watchReconnectMaxDelay(Duration.ofSeconds(10))
                .autoDeregisterOnClose(true)
                .build(),
        OpenTelemetry.noop()
);

RegistryWatchSubscription subscription = client.watchInstances(
        RegistryQueryRequest.builder()
                .namespace("prod")
                .organization("company")
                .businessDomain("trade")
                .capabilityDomain("order")
                .application("order-center")
                .role("api")
                .build(),
        new RegistryWatchListener() {
            @Override
            public void onEvent(RegistryWatchEvent event) {
                System.out.println(event);
            }
        }
);
```

聚合目录 watch：

```java
ServiceDirectorySubscription subscription = client.watchDirectory(
        RegistryWatchRequest.builder()
                .namespace("prod")
                .servicePrefixes(List.of("company.trade.order", "company.trade.payment"))
                .sinceRevision(1024L)
                .includeSnapshot(true)
                .build(),
        new RegistryWatchListener() {
            @Override
            public void onEvent(RegistryWatchEvent event) {
                System.out.println("event=" + event.getType() + ", revision=" + event.getRevision());
            }
        }
);

ServiceDirectory directory = subscription.getServiceDirectory();
List<RegistryInstance> instances = directory.listInstances("prod", "company.trade.order.order-center.api");
long revision = subscription.getLastRevision();
```

只维护本地目录缓存而不接收业务回调：

```java
ServiceDirectorySubscription subscription = client.watchDirectory(
        RegistryWatchRequest.builder()
                .namespace("prod")
                .organization("company")
                .businessDomain("trade")
                .capabilityDomain("order")
                .includeSnapshot(false)
                .build()
);

List<RegistryInstance> instances = subscription.getServiceDirectory()
        .listInstances("prod", "company.trade.order.order-center.api");
```

服务注册后托管定时心跳：

```java
HeartbeatSubscription heartbeatSubscription = client.registerAndScheduleHeartbeat(
        RegisterRequest.builder()
                .namespace("prod")
                .organization("company")
                .businessDomain("trade")
                .capabilityDomain("order")
                .application("order-center")
                .role("api")
                .instanceId("order-center-api-1")
                .endpoints(List.of(Endpoint.builder()
                        .protocol("http")
                        .host("127.0.0.1")
                        .port(8080)
                        .build()))
                .build(),
        Duration.ofSeconds(10)
);
```

这套 API 已经具备“聚合目录订阅 + 自动恢复 + 本地缓存”的基本能力，后续会继续增强更高层的路由和治理能力。
