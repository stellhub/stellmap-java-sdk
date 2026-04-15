package io.github.starmap;

import io.github.starmap.model.RegistryInstance;
import java.util.List;
import java.util.Map;

/** StarMap 本地服务目录只读视图。 */
public interface ServiceDirectory {

    /**
     * 返回当前目录最新 revision。
     *
     * @return 目录 revision
     */
    long getDirectoryRevision();

    /**
     * 读取某个服务的本地实例列表。
     *
     * @param namespace 命名空间
     * @param service 服务名
     * @return 实例列表
     */
    List<RegistryInstance> listInstances(String namespace, String service);

    /**
     * 返回某个服务的完整快照。
     *
     * @param namespace 命名空间
     * @param service 服务名
     * @return 服务快照，不存在时返回 null
     */
    ServiceSnapshot getSnapshot(String namespace, String service);

    /**
     * 返回当前目录的完整快照副本。
     *
     * @return 目录快照
     */
    Map<ServiceKey, ServiceSnapshot> snapshot();
}
