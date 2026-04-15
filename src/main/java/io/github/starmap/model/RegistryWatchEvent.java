package io.github.starmap.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** StarMap 注册中心 watch 事件。 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RegistryWatchEvent {

    private long revision;

    private String type;

    private String namespace;

    private String service;

    private String organization;

    private String businessDomain;

    private String capabilityDomain;

    private String application;

    private String role;

    private String instanceId;

    private RegistryInstance instance;

    private List<RegistryInstance> instances;
}
