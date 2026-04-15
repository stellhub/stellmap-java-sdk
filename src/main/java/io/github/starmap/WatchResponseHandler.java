package io.github.starmap;

import io.github.starmap.exception.StarMapTransportException;
import io.github.starmap.model.RegistryWatchEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 负责解析 watch SSE 响应流。
 */
final class WatchResponseHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final StarMapClient owner;
    private final ManagedDirectoryWatchSubscription subscription;
    private final HttpRequest request;
    private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errorBuffer = new ByteArrayOutputStream();
    private final StringBuilder data = new StringBuilder();
    private final boolean reconnectAttempt;
    private String eventName;
    private String eventId;
    private boolean streamOpened;
    private int statusCode;
    private Throwable terminalFailure;

    WatchResponseHandler(StarMapClient owner, ManagedDirectoryWatchSubscription subscription, HttpRequest request, boolean reconnectAttempt) {
        this.owner = owner;
        this.subscription = subscription;
        this.request = request;
        this.reconnectAttempt = reconnectAttempt;
    }

    @Override
    public void channelActive(ChannelHandlerContext context) {
        context.writeAndFlush(request).addListener(future -> {
            if (!future.isSuccess()) {
                subscription.handleConnectAttemptFailed(reconnectAttempt, new StarMapTransportException("Failed to send StarMap watch request", future.cause()));
                context.close();
            }
        });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, HttpObject message) throws Exception {
        if (message instanceof HttpResponse response) {
            statusCode = response.status().code();
            if (owner.transportInternal().isSuccess(statusCode)) {
                streamOpened = true;
                subscription.handleStreamOpen(context.channel());
            }
        }

        if (message instanceof HttpContent content) {
            if (!streamOpened) {
                errorBuffer.write(ByteBufUtil.getBytes(content.content()));
                if (content instanceof LastHttpContent) {
                    terminalFailure = owner.transportInternal().buildServerException(statusCode, errorBuffer.toString(StandardCharsets.UTF_8));
                    context.close();
                }
                return;
            }

            consumeSseBytes(content.content());
            if (content instanceof LastHttpContent) {
                finalizePendingLine();
                publishPendingEvent();
                context.close();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
        subscription.handleStreamClosed(context.channel(), reconnectAttempt, streamOpened, terminalFailure);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        terminalFailure = new StarMapTransportException("Failed to read StarMap watch stream", cause);
        context.close();
    }

    /**
     * Incrementally decodes SSE lines and keeps UTF-8 boundaries intact.
     *
     * @param content Netty content buffer
     */
    private void consumeSseBytes(ByteBuf content) throws IOException {
        byte[] bytes = ByteBufUtil.getBytes(content);
        for (byte currentByte : bytes) {
            if (currentByte == '\n') {
                processLineBytes();
                continue;
            }
            if (currentByte != '\r') {
                lineBuffer.write(currentByte);
            }
        }
    }

    /**
     * Flushes the last SSE line when the channel closes without trailing LF.
     */
    private void finalizePendingLine() {
        if (lineBuffer.size() > 0) {
            processLineBytes();
        }
    }

    /**
     * Decodes one complete SSE line and folds it into the current event frame.
     */
    private void processLineBytes() {
        String line = lineBuffer.toString(StandardCharsets.UTF_8);
        lineBuffer.reset();
        if (line.isEmpty()) {
            publishPendingEvent();
            return;
        }
        if (line.startsWith(":")) {
            return;
        }
        if (line.startsWith("event:")) {
            eventName = line.substring("event:".length()).trim();
            return;
        }
        if (line.startsWith("id:")) {
            eventId = line.substring("id:".length()).trim();
            return;
        }
        if (line.startsWith("data:")) {
            if (!data.isEmpty()) {
                data.append('\n');
            }
            data.append(line.substring("data:".length()).trim());
        }
    }

    /**
     * Publishes one completed watch event to the managed subscription.
     */
    private void publishPendingEvent() {
        if (data.isEmpty()) {
            eventName = null;
            eventId = null;
            return;
        }
        try {
            RegistryWatchEvent event = StarMapClient.OBJECT_MAPPER.readValue(data.toString(), RegistryWatchEvent.class);
            if (!owner.hasText(event.getType()) && owner.hasText(eventName)) {
                event.setType(eventName);
            }
            if (event.getRevision() == 0L && owner.hasText(eventId)) {
                event.setRevision(Long.parseLong(eventId));
            }
            subscription.handleEvent(event);
        } catch (Exception e) {
            subscription.handleNonTerminalError(new StarMapTransportException("Failed to decode StarMap watch event", e));
        } finally {
            data.setLength(0);
            eventName = null;
            eventId = null;
        }
    }
}
