package io.github.stellmap;

import io.github.stellmap.model.RegistryInstance;
import io.github.stellmap.model.RegistryWatchEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/** 默认本地目录缓存实现。 */
final class DefaultServiceDirectory implements ServiceDirectory {

    private static final String TYPE_SNAPSHOT = "SNAPSHOT";
    private static final String TYPE_UPSERT = "UPSERT";
    private static final String TYPE_DELETE = "DELETE";
    private static final String TYPE_RESET = "RESET";
    private static final String TYPE_ADDED = "ADDED";
    private static final String TYPE_UPDATED = "UPDATED";
    private static final String TYPE_REMOVED = "REMOVED";

    private final ConcurrentMap<ServiceKey, ServiceState> states = new ConcurrentHashMap<>();
    private final AtomicLong directoryRevision = new AtomicLong(0L);

    /**
     * 应用一个目录事件到本地缓存。
     *
     * @param event 目录事件
     */
    void apply(RegistryWatchEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        updateDirectoryRevision(event.getRevision());
        String normalizedType = normalizeType(event.getType());
        switch (normalizedType) {
            case TYPE_RESET -> reset(event);
            case TYPE_SNAPSHOT -> snapshot(event);
            case TYPE_DELETE, TYPE_REMOVED -> delete(event);
            case TYPE_UPSERT, TYPE_ADDED, TYPE_UPDATED -> upsert(event);
            default -> applyBestEffort(event);
        }
    }

    /** 清空目录缓存。 */
    void clear() {
        states.clear();
        directoryRevision.set(0L);
    }

    /**
     * 返回当前目录缓存统计。
     *
     * @return 目录缓存统计
     */
    DirectoryStats stats() {
        int serviceCount = states.size();
        int instanceCount = 0;
        for (ServiceState state : states.values()) {
            instanceCount += state.size();
        }
        return new DirectoryStats(serviceCount, instanceCount);
    }

    @Override
    public long getDirectoryRevision() {
        return directoryRevision.get();
    }

    @Override
    public List<RegistryInstance> listInstances(String namespace, String service) {
        ServiceSnapshot snapshot = getSnapshot(namespace, service);
        return snapshot == null ? List.of() : snapshot.getInstances();
    }

    @Override
    public ServiceSnapshot getSnapshot(String namespace, String service) {
        ServiceState state =
                states.get(ServiceKey.builder().namespace(namespace).service(service).build());
        if (state == null) {
            return null;
        }
        return state.toSnapshot();
    }

    @Override
    public Map<ServiceKey, ServiceSnapshot> snapshot() {
        Map<ServiceKey, ServiceSnapshot> copy = new LinkedHashMap<>();
        states.forEach((k, v) -> copy.put(k, v.toSnapshot()));
        return Map.copyOf(copy);
    }

    private void reset(RegistryWatchEvent event) {
        ServiceKey key = resolveEventKey(event);
        if (key == null) {
            clear();
            updateDirectoryRevision(event.getRevision());
            return;
        }
        states.remove(key);
    }

    private void snapshot(RegistryWatchEvent event) {
        Map<ServiceKey, List<RegistryInstance>> grouped = groupInstances(event);
        if (!grouped.isEmpty()) {
            grouped.forEach((key, instances) -> replaceSnapshot(key, instances, event.getRevision()));
            return;
        }
        ServiceKey key = resolveEventKey(event);
        if (key == null) {
            return;
        }
        List<RegistryInstance> instances =
                event.getInstance() == null ? List.of() : List.of(event.getInstance());
        replaceSnapshot(key, instances, event.getRevision());
    }

    private void upsert(RegistryWatchEvent event) {
        if (event.getInstances() != null && !event.getInstances().isEmpty()) {
            for (RegistryInstance instance : event.getInstances()) {
                upsertInstance(instance, event.getRevision());
            }
            return;
        }
        if (event.getInstance() != null) {
            upsertInstance(event.getInstance(), event.getRevision());
        }
    }

    private void delete(RegistryWatchEvent event) {
        if (event.getInstances() != null && !event.getInstances().isEmpty()) {
            for (RegistryInstance instance : event.getInstances()) {
                deleteInstance(
                        instance.getNamespace(),
                        instance.getService(),
                        instance.getInstanceId(),
                        event.getRevision());
            }
            return;
        }
        RegistryInstance instance = event.getInstance();
        if (instance != null) {
            deleteInstance(
                    instance.getNamespace(),
                    instance.getService(),
                    instance.getInstanceId(),
                    event.getRevision());
            return;
        }
        ServiceKey key = resolveEventKey(event);
        if (key == null) {
            return;
        }
        String instanceId = hasText(event.getInstanceId()) ? event.getInstanceId().trim() : null;
        if (instanceId == null) {
            states.remove(key);
            return;
        }
        deleteInstance(key.getNamespace(), key.getService(), instanceId, event.getRevision());
    }

    private void applyBestEffort(RegistryWatchEvent event) {
        if (event.getInstances() != null && !event.getInstances().isEmpty()) {
            snapshot(event);
            return;
        }
        if (event.getInstance() != null) {
            upsert(event);
        }
    }

    private Map<ServiceKey, List<RegistryInstance>> groupInstances(RegistryWatchEvent event) {
        if (event.getInstances() == null || event.getInstances().isEmpty()) {
            return Map.of();
        }
        Map<ServiceKey, List<RegistryInstance>> grouped = new LinkedHashMap<>();
        for (RegistryInstance instance : event.getInstances()) {
            if (instance == null) {
                continue;
            }
            ServiceKey key = resolveInstanceKey(instance);
            if (key == null) {
                continue;
            }
            grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(instance);
        }
        return grouped;
    }

    private void replaceSnapshot(ServiceKey key, List<RegistryInstance> instances, long revision) {
        ServiceState state = new ServiceState();
        state.replace(instances, revision);
        states.put(key, state);
    }

    private void upsertInstance(RegistryInstance instance, long revision) {
        ServiceKey key = resolveInstanceKey(instance);
        if (key == null || !hasText(instance.getInstanceId())) {
            return;
        }
        states
                .computeIfAbsent(key, ignored -> new ServiceState())
                .upsert(instance.getInstanceId().trim(), instance, revision);
    }

    private void deleteInstance(String namespace, String service, String instanceId, long revision) {
        if (!hasText(namespace) || !hasText(service) || !hasText(instanceId)) {
            return;
        }
        ServiceKey key =
                ServiceKey.builder().namespace(namespace.trim()).service(service.trim()).build();
        ServiceState state = states.get(key);
        if (state == null) {
            return;
        }
        state.delete(instanceId.trim(), revision);
        if (state.isEmpty()) {
            states.remove(key, state);
        }
    }

    private ServiceKey resolveEventKey(RegistryWatchEvent event) {
        if (hasText(event.getNamespace()) && hasText(event.getService())) {
            return ServiceKey.builder()
                    .namespace(event.getNamespace().trim())
                    .service(event.getService().trim())
                    .build();
        }
        if (event.getInstance() != null) {
            return resolveInstanceKey(event.getInstance());
        }
        return null;
    }

    private ServiceKey resolveInstanceKey(RegistryInstance instance) {
        if (instance == null || !hasText(instance.getNamespace()) || !hasText(instance.getService())) {
            return null;
        }
        return ServiceKey.builder()
                .namespace(instance.getNamespace().trim())
                .service(instance.getService().trim())
                .build();
    }

    private void updateDirectoryRevision(long revision) {
        if (revision <= 0L) {
            return;
        }
        directoryRevision.accumulateAndGet(revision, Math::max);
    }

    private String normalizeType(String type) {
        if (!hasText(type)) {
            return "";
        }
        return type.trim().toUpperCase();
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    /** Mutable internal state for one service. */
    private static final class ServiceState {

        private final ConcurrentMap<String, RegistryInstance> instancesById = new ConcurrentHashMap<>();
        private final AtomicLong revision = new AtomicLong(0L);

        private void replace(List<RegistryInstance> instances, long newRevision) {
            instancesById.clear();
            if (instances != null) {
                for (RegistryInstance instance : instances) {
                    if (instance != null && instance.getInstanceId() != null) {
                        instancesById.put(instance.getInstanceId(), instance);
                    }
                }
            }
            updateRevision(newRevision);
        }

        private void upsert(String instanceId, RegistryInstance instance, long newRevision) {
            instancesById.put(instanceId, instance);
            updateRevision(newRevision);
        }

        private void delete(String instanceId, long newRevision) {
            instancesById.remove(instanceId);
            updateRevision(newRevision);
        }

        private boolean isEmpty() {
            return instancesById.isEmpty();
        }

        private int size() {
            return instancesById.size();
        }

        private ServiceSnapshot toSnapshot() {
            List<RegistryInstance> instances = new ArrayList<>(instancesById.values());
            instances.sort(
                    Comparator.comparing(
                            RegistryInstance::getInstanceId, Comparator.nullsLast(String::compareTo)));
            return ServiceSnapshot.builder().revision(revision.get()).instances(instances).build();
        }

        private void updateRevision(long newRevision) {
            if (newRevision <= 0L) {
                return;
            }
            revision.accumulateAndGet(newRevision, Math::max);
        }
    }

    /**
     * 目录缓存统计信息。
     *
     * @param serviceCount 当前服务数
     * @param instanceCount 当前实例总数
     */
    record DirectoryStats(int serviceCount, int instanceCount) {}
}
