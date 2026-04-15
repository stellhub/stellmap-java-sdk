package io.github.starmap.exception;

/**
 * StarMap 传输层异常。
 */
public class StarMapTransportException extends StarMapException {

    public StarMapTransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public StarMapTransportException(String message) {
        super(message);
    }
}
