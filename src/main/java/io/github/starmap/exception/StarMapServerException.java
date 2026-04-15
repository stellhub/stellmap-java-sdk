package io.github.starmap.exception;

import io.github.starmap.model.StarMapErrorResponse;
import lombok.Getter;

/** StarMap 服务端异常响应。 */
@Getter
public class StarMapServerException extends StarMapException {

    private final int statusCode;
    private final StarMapErrorResponse errorResponse;

    public StarMapServerException(int statusCode, StarMapErrorResponse errorResponse) {
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
