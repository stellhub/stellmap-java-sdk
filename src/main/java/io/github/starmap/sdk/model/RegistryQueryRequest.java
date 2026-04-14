package io.github.starmap.sdk.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * StarMap 实例查询与 watch 过滤条件。
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RegistryQueryRequest {

    private String namespace;

    private String service;

    private String zone;

    private String endpoint;

    private List<String> selectors;

    private Map<String, String> labels;

    private Integer limit;
}
