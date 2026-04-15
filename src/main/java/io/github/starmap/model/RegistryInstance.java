package io.github.starmap.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** StarMap 实例视图。 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RegistryInstance {

    private String namespace;

    private String service;

    private String organization;

    private String businessDomain;

    private String capabilityDomain;

    private String application;

    private String role;

    private String instanceId;

    private String zone;

    private Map<String, String> labels;

    private Map<String, String> metadata;

    private List<Endpoint> endpoints;

    private long leaseTtlSeconds;

    private long registeredAtUnix;

    private long lastHeartbeatUnix;
}
