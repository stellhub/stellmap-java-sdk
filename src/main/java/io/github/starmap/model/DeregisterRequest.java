package io.github.starmap.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * StarMap 注销请求。
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DeregisterRequest {

    private String namespace;

    private String service;

    private String organization;

    private String businessDomain;

    private String capabilityDomain;

    private String application;

    private String role;

    private String instanceId;
}
