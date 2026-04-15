package io.github.starmap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.starmap.exception.StarMapServerException;
import io.github.starmap.exception.StarMapTransportException;
import io.github.starmap.model.StarMapErrorResponse;
import io.github.starmap.model.StarMapResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 基于 Netty 的 HTTP 传输层，负责请求发送、响应聚合与 leader 跟随重试。 */
final class NettyHttpTransport {

    private static final Logger log = LoggerFactory.getLogger(NettyHttpTransport.class);
    private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

    private final ObjectMapper objectMapper;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrapTemplate;
    private final Duration requestTimeout;
    private final Map<String, String> defaultHeaders;
    private final SslContext sslContext;
    private final StarMapClientMetrics metrics;
    private final boolean followLeaderRedirect;

    NettyHttpTransport(
            ObjectMapper objectMapper,
            EventLoopGroup eventLoopGroup,
            Bootstrap bootstrapTemplate,
            Duration requestTimeout,
            Map<String, String> defaultHeaders,
            SslContext sslContext,
            StarMapClientMetrics metrics,
            boolean followLeaderRedirect) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.eventLoopGroup = Objects.requireNonNull(eventLoopGroup, "eventLoopGroup must not be null");
        this.bootstrapTemplate =
                Objects.requireNonNull(bootstrapTemplate, "bootstrapTemplate must not be null");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        this.defaultHeaders = Objects.requireNonNull(defaultHeaders, "defaultHeaders must not be null");
        this.sslContext = Objects.requireNonNull(sslContext, "sslContext must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.followLeaderRedirect = followLeaderRedirect;
    }

    <T> StarMapResponse<T> executeJson(
            String method,
            URI requestBaseUri,
            String path,
            Object requestBody,
            JavaType responseType,
            boolean enableLeaderRedirect,
            int redirectsRemaining) {
        URI requestUri = resolve(requestBaseUri, path);
        byte[] bodyBytes = "GET".equals(method) ? null : toJsonBytes(requestBody);
        NettyHttpResponse response =
                executeRequest(method, requestUri, bodyBytes, "application/json", "application/json");

        try {
            if (isSuccess(response.statusCode())) {
                log.debug(
                        "StarMap request succeeded method={}, path={}, status={}",
                        method,
                        requestUri.getPath(),
                        response.statusCode());
                return objectMapper.readValue(response.body(), responseType);
            }

            StarMapServerException exception =
                    buildServerException(response.statusCode(), response.body());
            StarMapErrorResponse errorResponse = exception.getErrorResponse();
            if (enableLeaderRedirect
                    && followLeaderRedirect
                    && redirectsRemaining > 0
                    && errorResponse != null
                    && "not_leader".equals(errorResponse.getCode())
                    && hasText(errorResponse.getLeaderAddr())) {
                log.info(
                        "Redirecting StarMap write request to leader method={}, path={}, leaderAddr={},"
                                + " redirectsRemaining={}",
                        method,
                        requestUri.getPath(),
                        errorResponse.getLeaderAddr(),
                        redirectsRemaining);
                return executeJson(
                        method,
                        toLeaderBaseUri(requestBaseUri, errorResponse.getLeaderAddr()),
                        path,
                        requestBody,
                        responseType,
                        true,
                        redirectsRemaining - 1);
            }
            log.warn(
                    "StarMap request failed method={}, path={}, status={}, code={}",
                    method,
                    requestUri.getPath(),
                    response.statusCode(),
                    errorResponse == null ? null : errorResponse.getCode());
            throw exception;
        } catch (JsonProcessingException e) {
            log.warn(
                    "Failed to decode StarMap response method={}, path={}", method, requestUri.getPath(), e);
            throw new StarMapTransportException("Failed to decode StarMap response", e);
        }
    }

    Bootstrap newBootstrap(ChannelInitializer<SocketChannel> initializer) {
        Bootstrap bootstrap = bootstrapTemplate.clone();
        bootstrap.handler(initializer);
        return bootstrap;
    }

    HttpRequest buildWatchRequest(URI requestUri) {
        FullHttpRequest request =
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1, HttpMethod.GET, requestTarget(requestUri), Unpooled.EMPTY_BUFFER);
        HttpHeaders headers = request.headers();
        headers.set(HttpHeaderNames.HOST, hostHeader(requestUri));
        headers.set(HttpHeaderNames.ACCEPT, "text/event-stream");
        headers.set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_CACHE);
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        defaultHeaders.forEach(headers::add);
        HttpUtil.setContentLength(request, 0);
        return request;
    }

    void configureSsl(SocketChannel channel, URI requestUri, ChannelPipeline pipeline) {
        if (isHttps(requestUri)) {
            pipeline.addLast(
                    sslContext.newHandler(channel.alloc(), requestUri.getHost(), portOf(requestUri)));
        }
    }

    int portOf(URI requestUri) {
        if (requestUri.getPort() > 0) {
            return requestUri.getPort();
        }
        return isHttps(requestUri) ? 443 : 80;
    }

    URI resolve(URI currentBaseUri, String path) {
        return currentBaseUri.resolve(path.startsWith("/") ? path.substring(1) : path);
    }

    StarMapServerException buildServerException(int statusCode, String body) {
        try {
            StarMapErrorResponse errorResponse =
                    body == null || body.isBlank()
                            ? StarMapErrorResponse.builder().build()
                            : objectMapper.readValue(body, StarMapErrorResponse.class);
            return new StarMapServerException(statusCode, errorResponse);
        } catch (JsonProcessingException e) {
            return new StarMapServerException(
                    statusCode, StarMapErrorResponse.builder().message(body).build());
        }
    }

    boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    StarMapTransportException unwrapAsTransport(Throwable cause, String message) {
        if (cause instanceof StarMapServerException serverException) {
            throw serverException;
        }
        if (cause instanceof StarMapTransportException transportException) {
            return transportException;
        }
        return new StarMapTransportException(message, cause);
    }

    private NettyHttpResponse executeRequest(
            String method,
            URI requestUri,
            byte[] requestBody,
            String acceptHeader,
            String contentTypeHeader) {
        long startNanos = System.nanoTime();
        CompletableFuture<NettyHttpResponse> responseFuture = new CompletableFuture<>();
        AtomicReference<Channel> channelRef = new AtomicReference<>();
        Bootstrap bootstrap =
                newBootstrap(
                        new ChannelInitializer<>() {
                            @Override
                            protected void initChannel(SocketChannel channel) {
                                channelRef.set(channel);
                                ChannelPipeline pipeline = channel.pipeline();
                                configureSsl(channel, requestUri, pipeline);
                                pipeline.addLast(new HttpClientCodec());
                                pipeline.addLast(new HttpObjectAggregator(MAX_RESPONSE_BYTES));
                                pipeline.addLast(
                                        new AggregatedResponseHandler(
                                                buildJsonRequest(
                                                        method, requestUri, requestBody, acceptHeader, contentTypeHeader),
                                                responseFuture));
                            }
                        });

        ChannelFuture connectFuture = bootstrap.connect(requestUri.getHost(), portOf(requestUri));
        connectFuture.addListener(
                future -> {
                    if (!future.isSuccess()) {
                        responseFuture.completeExceptionally(
                                new StarMapTransportException("Failed to call StarMap HTTP API", future.cause()));
                    }
                });

        ScheduledFuture<?> timeoutFuture =
                scheduleTimeout(responseFuture, channelRef, "Timed out while calling StarMap HTTP API");
        try {
            NettyHttpResponse response = responseFuture.get();
            metrics.recordRequest(
                    method,
                    requestUri.getPath(),
                    response.statusCode(),
                    System.nanoTime() - startNanos,
                    isSuccess(response.statusCode()));
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.recordRequest(method, requestUri.getPath(), 0, System.nanoTime() - startNanos, false);
            log.debug("StarMap request interrupted method={}, uri={}", method, requestUri, e);
            throw new StarMapTransportException("Interrupted while calling StarMap HTTP API", e);
        } catch (ExecutionException e) {
            metrics.recordRequest(method, requestUri.getPath(), 0, System.nanoTime() - startNanos, false);
            log.warn(
                    "StarMap request execution failed method={}, uri={}", method, requestUri, e.getCause());
            throw unwrapAsTransport(e.getCause(), "Failed to call StarMap HTTP API");
        } finally {
            timeoutFuture.cancel(false);
        }
    }

    private ScheduledFuture<?> scheduleTimeout(
            CompletableFuture<?> future, AtomicReference<Channel> channelRef, String message) {
        return eventLoopGroup
                .next()
                .schedule(
                        () -> {
                            if (future.completeExceptionally(new StarMapTransportException(message))) {
                                Channel channel = channelRef.get();
                                if (channel != null) {
                                    channel.close();
                                }
                            }
                        },
                        requestTimeout.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    private FullHttpRequest buildJsonRequest(
            String method,
            URI requestUri,
            byte[] requestBody,
            String acceptHeader,
            String contentTypeHeader) {
        ByteBuf content =
                requestBody == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(requestBody);
        FullHttpRequest request =
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), requestTarget(requestUri), content);
        HttpHeaders headers = request.headers();
        headers.set(HttpHeaderNames.HOST, hostHeader(requestUri));
        headers.set(HttpHeaderNames.ACCEPT, acceptHeader);
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        defaultHeaders.forEach(headers::add);
        if (requestBody != null) {
            headers.set(HttpHeaderNames.CONTENT_TYPE, contentTypeHeader);
        }
        HttpUtil.setContentLength(request, content.readableBytes());
        return request;
    }

    private URI toLeaderBaseUri(URI currentBaseUri, String leaderAddr) {
        String normalized = leaderAddr.trim();
        if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            return normalizeBaseUri(normalized);
        }
        return normalizeBaseUri(currentBaseUri.getScheme() + "://" + normalized);
    }

    private URI normalizeBaseUri(String baseUrl) {
        String normalized = baseUrl.trim();
        if (!normalized.endsWith("/")) {
            normalized = normalized + "/";
        }
        return URI.create(normalized);
    }

    private byte[] toJsonBytes(Object requestBody) {
        try {
            return objectMapper.writeValueAsBytes(requestBody);
        } catch (JsonProcessingException e) {
            throw new StarMapTransportException("Failed to encode StarMap request body", e);
        }
    }

    private boolean isHttps(URI requestUri) {
        return "https".equalsIgnoreCase(requestUri.getScheme());
    }

    private String hostHeader(URI requestUri) {
        int port = portOf(requestUri);
        boolean defaultPort =
                (!isHttps(requestUri) && port == 80) || (isHttps(requestUri) && port == 443);
        return defaultPort ? requestUri.getHost() : requestUri.getHost() + ":" + port;
    }

    private String requestTarget(URI requestUri) {
        String path = requestUri.getRawPath();
        if (path == null || path.isBlank()) {
            path = "/";
        }
        String query = requestUri.getRawQuery();
        return query == null || query.isBlank() ? path : path + "?" + query;
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static final class AggregatedResponseHandler
            extends SimpleChannelInboundHandler<FullHttpResponse> {

        private final FullHttpRequest request;
        private final CompletableFuture<NettyHttpResponse> responseFuture;
        private boolean responseReceived;

        private AggregatedResponseHandler(
                FullHttpRequest request, CompletableFuture<NettyHttpResponse> responseFuture) {
            this.request = request;
            this.responseFuture = responseFuture;
        }

        @Override
        public void channelActive(ChannelHandlerContext context) {
            context
                    .writeAndFlush(request)
                    .addListener(
                            future -> {
                                if (!future.isSuccess()) {
                                    responseFuture.completeExceptionally(
                                            new StarMapTransportException(
                                                    "Failed to send StarMap HTTP request", future.cause()));
                                    context.close();
                                }
                            });
        }

        @Override
        protected void channelRead0(ChannelHandlerContext context, FullHttpResponse response) {
            responseReceived = true;
            String body = response.content().toString(StandardCharsets.UTF_8);
            responseFuture.complete(new NettyHttpResponse(response.status().code(), body));
            context.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) {
            if (!responseReceived) {
                responseFuture.completeExceptionally(
                        new StarMapTransportException("Connection closed before receiving StarMap response"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            responseFuture.completeExceptionally(
                    new StarMapTransportException("Failed to call StarMap HTTP API", cause));
            context.close();
        }
    }

    private record NettyHttpResponse(int statusCode, String body) {}
}
