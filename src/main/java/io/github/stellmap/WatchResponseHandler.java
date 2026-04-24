package io.github.stellmap;

import io.github.stellmap.exception.StellMapTransportException;
import io.github.stellmap.model.RegistryWatchEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import okhttp3.Response;
import okhttp3.ResponseBody;

/** 负责解析 watch SSE 响应流。 */
final class WatchResponseHandler {

    private final StellMapClient owner;
    private final ManagedDirectoryWatchSubscription subscription;
    private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();
    private final StringBuilder data = new StringBuilder();
    private final boolean reconnectAttempt;
    private String eventName;
    private String eventId;

    WatchResponseHandler(
            StellMapClient owner,
            ManagedDirectoryWatchSubscription subscription,
            boolean reconnectAttempt) {
        this.owner = owner;
        this.subscription = subscription;
        this.reconnectAttempt = reconnectAttempt;
    }

    /**
     * 读取并解析 SSE 响应流。
     *
     * @param response OkHttp 响应
     */
    void handle(Response response) {
        boolean streamOpened = false;
        Throwable terminalFailure = null;
        try {
            if (!owner.transportInternal().isSuccess(response.code())) {
                terminalFailure =
                        owner.transportInternal()
                                .buildServerException(response.code(), readBody(response.body()));
                subscription.handleConnectAttemptFailed(reconnectAttempt, terminalFailure);
                return;
            }

            ResponseBody body = response.body();
            if (body == null) {
                terminalFailure = new StellMapTransportException("StarMap watch stream body is empty");
                subscription.handleConnectAttemptFailed(reconnectAttempt, terminalFailure);
                return;
            }

            streamOpened = true;
            subscription.handleStreamOpen();
            readSse(body.byteStream());
        } catch (IOException e) {
            terminalFailure = new StellMapTransportException("Failed to read StarMap watch stream", e);
        } catch (RuntimeException e) {
            terminalFailure = new StellMapTransportException("Failed to process StarMap watch stream", e);
        } finally {
            if (streamOpened) {
                finalizePendingLine();
                publishPendingEvent();
                subscription.handleStreamClosed(reconnectAttempt, true, terminalFailure);
            }
        }
    }

    /**
     * 增量解析 SSE 字节流，保留 UTF-8 边界。
     *
     * @param inputStream 响应输入流
     * @throws IOException IO 异常
     */
    private void readSse(InputStream inputStream) throws IOException {
        byte[] chunk = new byte[1024];
        int read;
        while ((read = inputStream.read(chunk)) >= 0) {
            for (int index = 0; index < read; index++) {
                byte currentByte = chunk[index];
                if (currentByte == '\n') {
                    processLineBytes();
                    continue;
                }
                if (currentByte != '\r') {
                    lineBuffer.write(currentByte);
                }
            }
        }
    }

    /** Flushes the last SSE line when the stream closes without trailing LF. */
    private void finalizePendingLine() {
        if (lineBuffer.size() > 0) {
            processLineBytes();
        }
    }

    /** Decodes one complete SSE line and folds it into the current event frame. */
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

    /** Publishes one completed watch event to the managed subscription. */
    private void publishPendingEvent() {
        if (data.isEmpty()) {
            eventName = null;
            eventId = null;
            return;
        }
        try {
            RegistryWatchEvent event =
                    StellMapClient.OBJECT_MAPPER.readValue(data.toString(), RegistryWatchEvent.class);
            if (!owner.hasText(event.getType()) && owner.hasText(eventName)) {
                event.setType(eventName);
            }
            if (event.getRevision() == 0L && owner.hasText(eventId)) {
                event.setRevision(Long.parseLong(eventId));
            }
            subscription.handleEvent(event);
        } catch (Exception e) {
            subscription.handleNonTerminalError(
                    new StellMapTransportException("Failed to decode StarMap watch event", e));
        } finally {
            data.setLength(0);
            eventName = null;
            eventId = null;
        }
    }

    private String readBody(ResponseBody body) throws IOException {
        if (body == null) {
            return "";
        }
        return body.string();
    }
}
