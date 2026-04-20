package io.github.stellmap.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** StarMap 目录 watch 请求。 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RegistryWatchRequest {

    private String namespace;

    private String service;

    private List<String> services;

    private List<String> servicePrefixes;

    private String organization;

    private String businessDomain;

    private String capabilityDomain;

    private String application;

    private String role;

    private String zone;

    private String endpoint;

    private List<String> selectors;

    private Map<String, String> labels;

    @Builder.Default private long sinceRevision = 0L;

    @Builder.Default private boolean includeSnapshot = true;
}
