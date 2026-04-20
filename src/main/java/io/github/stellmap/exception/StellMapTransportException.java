package io.github.stellmap.exception;

/** StarMap 传输层异常。 */
public class StellMapTransportException extends StellMapException {

    public StellMapTransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public StellMapTransportException(String message) {
        super(message);
    }
}
