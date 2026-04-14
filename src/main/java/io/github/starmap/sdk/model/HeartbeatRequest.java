package io.github.starmap.sdk.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * StarMap 心跳续约请求。
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class HeartbeatRequest {

    private String namespace;

    private String service;

    private String instanceId;

    private long leaseTtlSeconds;
}
