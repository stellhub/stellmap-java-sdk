package io.github.stellmap.exception;

/** StarMap SDK 基础异常。 */
public class StellMapException extends RuntimeException {

    public StellMapException(String message) {
        super(message);
    }

    public StellMapException(String message, Throwable cause) {
        super(message, cause);
    }
}
