package io.github.starmap.sdk.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * StarMap 注册中心 watch 事件。
 */
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

    private String instanceId;

    private RegistryInstance instance;

    private List<RegistryInstance> instances;
}
