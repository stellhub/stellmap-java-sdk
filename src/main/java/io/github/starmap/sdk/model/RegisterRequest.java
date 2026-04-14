package io.github.starmap.sdk.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * StarMap 注册请求。
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RegisterRequest {

    private String namespace;

    private String service;

    private String instanceId;

    private String zone;

    private Map<String, String> labels;

    private Map<String, String> metadata;

    private List<Endpoint> endpoints;

    private long leaseTtlSeconds;
}
