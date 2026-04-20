package io.github.stellmap;

import io.github.stellmap.model.DeregisterRequest;
import io.github.stellmap.model.HeartbeatRequest;
import io.github.stellmap.model.RegisterRequest;
import java.util.Objects;

/** 注册实例唯一键。 */
final class RegistrationKey {

    private final String namespace;
    private final String service;
    private final String instanceId;

    private RegistrationKey(String namespace, String service, String instanceId) {
        this.namespace = namespace;
        this.service = service;
        this.instanceId = instanceId;
    }

    /**
     * 根据注册请求创建唯一键。
     *
     * @param request 注册请求
     * @return 注册唯一键
     */
    static RegistrationKey from(RegisterRequest request) {
        return new RegistrationKey(
                request.getNamespace(), request.getService(), request.getInstanceId());
    }

    /**
     * 根据注销请求创建唯一键。
     *
     * @param request 注销请求
     * @return 注册唯一键
     */
    static RegistrationKey from(DeregisterRequest request) {
        return new RegistrationKey(
                request.getNamespace(), request.getService(), request.getInstanceId());
    }

    /**
     * 根据心跳请求创建唯一键。
     *
     * @param request 心跳请求
     * @return 注册唯一键
     */
    static RegistrationKey from(HeartbeatRequest request) {
        return new RegistrationKey(
                request.getNamespace(), request.getService(), request.getInstanceId());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RegistrationKey that)) {
            return false;
        }
        return Objects.equals(namespace, that.namespace)
                && Objects.equals(service, that.service)
                && Objects.equals(instanceId, that.instanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, service, instanceId);
    }
}
