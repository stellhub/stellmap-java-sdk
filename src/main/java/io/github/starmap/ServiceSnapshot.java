package io.github.starmap;

import io.github.starmap.model.RegistryInstance;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

/**
 * 单个服务的本地快照。
 */
@Value
@Builder(toBuilder = true)
public class ServiceSnapshot {

    long revision;

    @Singular("instance")
    List<RegistryInstance> instances;
}
