package io.github.stellmap.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * StarMap 统一成功响应。
 *
 * @param <T> data 字段类型
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class StarMapResponse<T> {

    private String code;

    private String message;

    private T data;

    private String requestId;
}
