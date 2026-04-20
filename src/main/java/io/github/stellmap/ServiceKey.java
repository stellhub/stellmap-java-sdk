package io.github.stellmap;

import lombok.Builder;
import lombok.Value;

/** 服务目录缓存主键。 */
@Value
@Builder(toBuilder = true)
public class ServiceKey {

    String namespace;

    String service;
}
