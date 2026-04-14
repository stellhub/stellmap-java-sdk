package io.github.starmap.sdk.exception;

/**
 * StarMap SDK 基础异常。
 */
public class StarMapException extends RuntimeException {

    public StarMapException(String message) {
        super(message);
    }

    public StarMapException(String message, Throwable cause) {
        super(message, cause);
    }
}
