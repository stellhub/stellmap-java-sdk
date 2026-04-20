package io.github.stellmap.exception;

import io.github.stellmap.model.StarMapErrorResponse;
import lombok.Getter;

/** StarMap 服务端异常响应。 */
@Getter
public class StellMapServerException extends StellMapException {

    private final int statusCode;
    private final StarMapErrorResponse errorResponse;

    public StellMapServerException(int statusCode, StarMapErrorResponse errorResponse) {
        super(buildMessage(statusCode, errorResponse));
        this.statusCode = statusCode;
        this.errorResponse = errorResponse;
    }

    private static String buildMessage(int statusCode, StarMapErrorResponse errorResponse) {
        if (errorResponse == null) {
            return "StarMap server request failed, status=" + statusCode;
        }
        return "StarMap server request failed, status="
                + statusCode
                + ", code="
                + errorResponse.getCode()
                + ", message="
                + errorResponse.getMessage();
    }
}
