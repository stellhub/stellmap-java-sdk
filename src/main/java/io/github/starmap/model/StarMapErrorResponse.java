package io.github.starmap.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * StarMap 统一错误响应。
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class StarMapErrorResponse {

    private String code;

    private String message;

    private String requestId;

    private Long leaderId;

    private String leaderAddr;
}
