package io.github.stellmap;

import io.github.stellmap.model.RegistryInstance;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/** 单个服务的本地快照。 */
@Value
@Builder(toBuilder = true)
public class ServiceSnapshot {

    long revision;

    @Singular("instance")
    List<RegistryInstance> instances;
}
