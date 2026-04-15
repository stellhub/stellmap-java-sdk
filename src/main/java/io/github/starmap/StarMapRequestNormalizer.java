package io.github.starmap;

import io.github.starmap.model.DeregisterRequest;
import io.github.starmap.model.Endpoint;
import io.github.starmap.model.HeartbeatRequest;
import io.github.starmap.model.RegisterRequest;
import io.github.starmap.model.RegistryQueryRequest;
import io.github.starmap.model.RegistryWatchRequest;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 请求参数归一化与服务标识组装器。 */
final class StarMapRequestNormalizer {

    /**
     * 归一化注册请求。
     *
     * @param request 原始注册请求
     * @return 归一化后的注册请求
     */
    RegisterRequest normalizeRegisterRequest(RegisterRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getLeaseTtlSeconds() < 0) {
            throw new IllegalArgumentException("leaseTtlSeconds must be greater than or equal to 0");
        }
        List<Endpoint> endpoints = normalizeEndpoints(request.getEndpoints());
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one endpoint is required");
        }
        ExactServiceIdentity serviceIdentity =
                normalizeExactServiceIdentity(
                        request.getService(),
                        request.getOrganization(),
                        request.getBusinessDomain(),
                        request.getCapabilityDomain(),
                        request.getApplication(),
                        request.getRole(),
                        "service");
        return RegisterRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(serviceIdentity.service())
                .organization(serviceIdentity.organization())
                .businessDomain(serviceIdentity.businessDomain())
                .capabilityDomain(serviceIdentity.capabilityDomain())
                .application(serviceIdentity.application())
                .role(serviceIdentity.role())
                .instanceId(requireText(request.getInstanceId(), "instanceId"))
                .zone(trimToNull(request.getZone()))
                .labels(normalizeMap(request.getLabels()))
                .metadata(normalizeMap(request.getMetadata()))
                .endpoints(endpoints)
                .leaseTtlSeconds(
                        request.getLeaseTtlSeconds() > 0
                                ? request.getLeaseTtlSeconds()
                                : StarMapDefaults.DEFAULT_LEASE_TTL_SECONDS)
                .build();
    }

    /**
     * 归一化注销请求。
     *
     * @param request 原始注销请求
     * @return 归一化后的注销请求
     */
    DeregisterRequest normalizeDeregisterRequest(DeregisterRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        ExactServiceIdentity serviceIdentity =
                normalizeExactServiceIdentity(
                        request.getService(),
                        request.getOrganization(),
                        request.getBusinessDomain(),
                        request.getCapabilityDomain(),
                        request.getApplication(),
                        request.getRole(),
                        "service");
        return DeregisterRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(serviceIdentity.service())
                .organization(serviceIdentity.organization())
                .businessDomain(serviceIdentity.businessDomain())
                .capabilityDomain(serviceIdentity.capabilityDomain())
                .application(serviceIdentity.application())
                .role(serviceIdentity.role())
                .instanceId(requireText(request.getInstanceId(), "instanceId"))
                .build();
    }

    /**
     * 归一化心跳请求。
     *
     * @param request 原始心跳请求
     * @return 归一化后的心跳请求
     */
    HeartbeatRequest normalizeHeartbeatRequest(HeartbeatRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getLeaseTtlSeconds() < 0) {
            throw new IllegalArgumentException("leaseTtlSeconds must be greater than or equal to 0");
        }
        ExactServiceIdentity serviceIdentity =
                normalizeExactServiceIdentity(
                        request.getService(),
                        request.getOrganization(),
                        request.getBusinessDomain(),
                        request.getCapabilityDomain(),
                        request.getApplication(),
                        request.getRole(),
                        "service");
        return HeartbeatRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(serviceIdentity.service())
                .organization(serviceIdentity.organization())
                .businessDomain(serviceIdentity.businessDomain())
                .capabilityDomain(serviceIdentity.capabilityDomain())
                .application(serviceIdentity.application())
                .role(serviceIdentity.role())
                .instanceId(requireText(request.getInstanceId(), "instanceId"))
                .leaseTtlSeconds(request.getLeaseTtlSeconds())
                .build();
    }

    /**
     * 归一化用于定时调度的心跳请求。
     *
     * @param request 原始心跳请求
     * @return 归一化后的心跳请求
     */
    HeartbeatRequest normalizeHeartbeatRequestForScheduling(HeartbeatRequest request) {
        HeartbeatRequest normalized = normalizeHeartbeatRequest(request);
        if (normalized.getLeaseTtlSeconds() > 0) {
            return normalized;
        }
        return normalized.toBuilder()
                .leaseTtlSeconds(StarMapDefaults.DEFAULT_LEASE_TTL_SECONDS)
                .build();
    }

    /**
     * 归一化实例查询请求。
     *
     * @param request 原始查询请求
     * @return 归一化后的查询请求
     */
    RegistryQueryRequest normalizeRegistryQueryRequest(RegistryQueryRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getLimit() != null && request.getLimit() < 0) {
            throw new IllegalArgumentException("limit must be greater than or equal to 0");
        }
        ServiceFilterProjection serviceFilter =
                normalizeServiceFilter(
                        request.getService(),
                        request.getServices(),
                        request.getServicePrefixes(),
                        request.getOrganization(),
                        request.getBusinessDomain(),
                        request.getCapabilityDomain(),
                        request.getApplication(),
                        request.getRole());
        return RegistryQueryRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(serviceFilter.service())
                .services(serviceFilter.services())
                .servicePrefixes(serviceFilter.servicePrefixes())
                .organization(serviceFilter.organization())
                .businessDomain(serviceFilter.businessDomain())
                .capabilityDomain(serviceFilter.capabilityDomain())
                .application(serviceFilter.application())
                .role(serviceFilter.role())
                .zone(trimToNull(request.getZone()))
                .endpoint(trimToNull(request.getEndpoint()))
                .selectors(normalizeStrings(request.getSelectors()))
                .labels(normalizeMap(request.getLabels()))
                .limit(request.getLimit())
                .build();
    }

    /**
     * 归一化 watch 请求。
     *
     * @param request 原始 watch 请求
     * @return 归一化后的 watch 请求
     */
    RegistryWatchRequest normalizeWatchRequest(RegistryWatchRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        if (request.getSinceRevision() < 0L) {
            throw new IllegalArgumentException("sinceRevision must be greater than or equal to 0");
        }
        ServiceFilterProjection serviceFilter =
                normalizeServiceFilter(
                        request.getService(),
                        request.getServices(),
                        request.getServicePrefixes(),
                        request.getOrganization(),
                        request.getBusinessDomain(),
                        request.getCapabilityDomain(),
                        request.getApplication(),
                        request.getRole());
        return RegistryWatchRequest.builder()
                .namespace(requireText(request.getNamespace(), "namespace"))
                .service(serviceFilter.service())
                .services(serviceFilter.services())
                .servicePrefixes(serviceFilter.servicePrefixes())
                .organization(serviceFilter.organization())
                .businessDomain(serviceFilter.businessDomain())
                .capabilityDomain(serviceFilter.capabilityDomain())
                .application(serviceFilter.application())
                .role(serviceFilter.role())
                .zone(trimToNull(request.getZone()))
                .endpoint(trimToNull(request.getEndpoint()))
                .selectors(normalizeStrings(request.getSelectors()))
                .labels(normalizeMap(request.getLabels()))
                .sinceRevision(request.getSinceRevision())
                .includeSnapshot(request.isIncludeSnapshot())
                .build();
    }

    /**
     * 将实例查询请求转换成 watch 请求。
     *
     * @param request 已归一化的查询请求
     * @return watch 请求
     */
    RegistryWatchRequest toWatchRequest(RegistryQueryRequest request) {
        List<String> services =
                request.getServices() != null
                        ? request.getServices()
                        : (hasText(request.getService()) ? List.of(request.getService()) : null);
        return RegistryWatchRequest.builder()
                .namespace(request.getNamespace())
                .service(request.getService())
                .services(services)
                .servicePrefixes(request.getServicePrefixes())
                .organization(request.getOrganization())
                .businessDomain(request.getBusinessDomain())
                .capabilityDomain(request.getCapabilityDomain())
                .application(request.getApplication())
                .role(request.getRole())
                .zone(request.getZone())
                .endpoint(request.getEndpoint())
                .selectors(request.getSelectors())
                .labels(request.getLabels())
                .includeSnapshot(true)
                .build();
    }

    /**
     * 构建实例查询参数。
     *
     * @param request 已归一化的查询请求
     * @return 查询字符串
     */
    String buildRegistryQuery(RegistryQueryRequest request) {
        List<String> items = new ArrayList<>();
        addQuery(items, "namespace", request.getNamespace());
        addQuery(items, "service", request.getService());
        if (request.getServices() != null) {
            for (String service : request.getServices()) {
                addQuery(items, "service", service);
            }
        }
        if (request.getServicePrefixes() != null) {
            for (String servicePrefix : request.getServicePrefixes()) {
                addQuery(items, "servicePrefix", servicePrefix);
            }
        }
        addQuery(items, "organization", request.getOrganization());
        addQuery(items, "businessDomain", request.getBusinessDomain());
        addQuery(items, "capabilityDomain", request.getCapabilityDomain());
        addQuery(items, "application", request.getApplication());
        addQuery(items, "role", request.getRole());
        addQuery(items, "zone", request.getZone());
        addQuery(items, "endpoint", request.getEndpoint());
        if (request.getLimit() != null && request.getLimit() > 0) {
            addQuery(items, "limit", String.valueOf(request.getLimit()));
        }
        if (request.getSelectors() != null) {
            for (String selector : request.getSelectors()) {
                addQuery(items, "selector", selector);
            }
        }
        if (request.getLabels() != null) {
            for (Map.Entry<String, String> entry : request.getLabels().entrySet()) {
                addQuery(items, "label", entry.getKey() + "=" + entry.getValue());
            }
        }
        return items.isEmpty() ? "" : "?" + String.join("&", items);
    }

    /**
     * 构建 watch 查询参数。
     *
     * @param request 已归一化的 watch 请求
     * @param sinceRevision 起始 revision
     * @param includeSnapshot 是否包含快照
     * @return 查询字符串
     */
    String buildWatchQuery(
            RegistryWatchRequest request, long sinceRevision, boolean includeSnapshot) {
        List<String> items = new ArrayList<>();
        addQuery(items, "namespace", request.getNamespace());
        addQuery(items, "service", request.getService());
        addQuery(items, "organization", request.getOrganization());
        addQuery(items, "businessDomain", request.getBusinessDomain());
        addQuery(items, "capabilityDomain", request.getCapabilityDomain());
        addQuery(items, "application", request.getApplication());
        addQuery(items, "role", request.getRole());
        addQuery(items, "zone", request.getZone());
        addQuery(items, "endpoint", request.getEndpoint());
        if (request.getServices() != null) {
            for (String service : request.getServices()) {
                addQuery(items, "service", service);
            }
        }
        if (request.getServicePrefixes() != null) {
            for (String servicePrefix : request.getServicePrefixes()) {
                addQuery(items, "servicePrefix", servicePrefix);
            }
        }
        if (request.getSelectors() != null) {
            for (String selector : request.getSelectors()) {
                addQuery(items, "selector", selector);
            }
        }
        if (request.getLabels() != null) {
            for (Map.Entry<String, String> entry : request.getLabels().entrySet()) {
                addQuery(items, "label", entry.getKey() + "=" + entry.getValue());
            }
        }
        if (sinceRevision > 0L) {
            addQuery(items, "sinceRevision", Long.toString(sinceRevision));
        }
        addQuery(items, "includeSnapshot", Boolean.toString(includeSnapshot));
        return items.isEmpty() ? "" : "?" + String.join("&", items);
    }

    private void addQuery(List<String> items, String key, String value) {
        if (!hasText(value)) {
            return;
        }
        items.add(urlEncode(key) + "=" + urlEncode(value.trim()));
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private List<Endpoint> normalizeEndpoints(List<Endpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return List.of();
        }
        Map<String, Endpoint> result = new LinkedHashMap<>();
        for (Endpoint endpoint : endpoints) {
            if (endpoint == null) {
                continue;
            }
            String protocol = requireText(endpoint.getProtocol(), "endpoint.protocol");
            String host = requireText(endpoint.getHost(), "endpoint.host");
            if (endpoint.getPort() <= 0 || endpoint.getPort() > 65535) {
                throw new IllegalArgumentException("endpoint.port must be within 1..65535");
            }
            if (endpoint.getWeight() < 0) {
                throw new IllegalArgumentException("endpoint.weight must be greater than or equal to 0");
            }
            String name = hasText(endpoint.getName()) ? endpoint.getName().trim() : protocol;
            if (result.containsKey(name)) {
                throw new IllegalArgumentException("duplicate endpoint name: " + name);
            }
            result.put(
                    name,
                    Endpoint.builder()
                            .name(name)
                            .protocol(protocol)
                            .host(host)
                            .port(endpoint.getPort())
                            .path(trimToNull(endpoint.getPath()))
                            .weight(
                                    endpoint.getWeight() > 0
                                            ? endpoint.getWeight()
                                            : StarMapDefaults.DEFAULT_ENDPOINT_WEIGHT)
                            .build());
        }
        return List.copyOf(result.values());
    }

    private Map<String, String> normalizeMap(Map<String, String> source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        Map<String, String> result = new LinkedHashMap<>();
        source.forEach(
                (k, v) -> {
                    String normalizedKey = trimToNull(k);
                    if (normalizedKey != null) {
                        result.put(normalizedKey, v == null ? "" : v.trim());
                    }
                });
        return result.isEmpty() ? null : Map.copyOf(result);
    }

    private List<String> normalizeStrings(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        List<String> result =
                values.stream().map(this::trimToNull).filter(Objects::nonNull).distinct().toList();
        return result.isEmpty() ? null : result;
    }

    private ExactServiceIdentity normalizeExactServiceIdentity(
            String service,
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role,
            String fieldPrefix) {
        String normalizedService = trimToNull(service);
        String normalizedOrganization = trimToNull(organization);
        String normalizedBusinessDomain = trimToNull(businessDomain);
        String normalizedCapabilityDomain = trimToNull(capabilityDomain);
        String normalizedApplication = trimToNull(application);
        String normalizedRole = trimToNull(role);

        String[] parsed =
                normalizedService == null ? null : parseCanonicalServiceName(normalizedService);
        if (normalizedOrganization == null && parsed != null) {
            normalizedOrganization = parsed[0];
        }
        if (normalizedBusinessDomain == null && parsed != null) {
            normalizedBusinessDomain = parsed[1];
        }
        if (normalizedCapabilityDomain == null && parsed != null) {
            normalizedCapabilityDomain = parsed[2];
        }
        if (normalizedApplication == null && parsed != null) {
            normalizedApplication = parsed[3];
        }
        if (normalizedRole == null && parsed != null) {
            normalizedRole = parsed[4];
        }

        if (hasAnyText(
                normalizedOrganization,
                normalizedBusinessDomain,
                normalizedCapabilityDomain,
                normalizedApplication,
                normalizedRole)) {
            if (!hasAllText(
                    normalizedOrganization,
                    normalizedBusinessDomain,
                    normalizedCapabilityDomain,
                    normalizedApplication,
                    normalizedRole)) {
                throw new IllegalArgumentException(
                        fieldPrefix
                                + " hierarchy must include organization, businessDomain, capabilityDomain,"
                                + " application and role");
            }
            String canonicalService =
                    composeCanonicalServiceName(
                            normalizedOrganization,
                            normalizedBusinessDomain,
                            normalizedCapabilityDomain,
                            normalizedApplication,
                            normalizedRole);
            if (normalizedService != null && !normalizedService.equals(canonicalService)) {
                throw new IllegalArgumentException(
                        fieldPrefix + " must match structured service identity: " + canonicalService);
            }
            normalizedService = canonicalService;
        }

        if (normalizedService == null) {
            throw new IllegalArgumentException(fieldPrefix + " must not be blank");
        }
        return new ExactServiceIdentity(
                normalizedService,
                normalizedOrganization,
                normalizedBusinessDomain,
                normalizedCapabilityDomain,
                normalizedApplication,
                normalizedRole);
    }

    private ServiceFilterProjection normalizeServiceFilter(
            String service,
            List<String> services,
            List<String> servicePrefixes,
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role) {
        String normalizedService = trimToNull(service);
        List<String> normalizedServices = normalizeStrings(services);
        List<String> normalizedPrefixes = normalizeStrings(servicePrefixes);
        String normalizedOrganization = trimToNull(organization);
        String normalizedBusinessDomain = trimToNull(businessDomain);
        String normalizedCapabilityDomain = trimToNull(capabilityDomain);
        String normalizedApplication = trimToNull(application);
        String normalizedRole = trimToNull(role);

        String[] parsed =
                normalizedService == null ? null : parseCanonicalServiceName(normalizedService);
        if (normalizedOrganization == null && parsed != null) {
            normalizedOrganization = parsed[0];
        }
        if (normalizedBusinessDomain == null && parsed != null) {
            normalizedBusinessDomain = parsed[1];
        }
        if (normalizedCapabilityDomain == null && parsed != null) {
            normalizedCapabilityDomain = parsed[2];
        }
        if (normalizedApplication == null && parsed != null) {
            normalizedApplication = parsed[3];
        }
        if (normalizedRole == null && parsed != null) {
            normalizedRole = parsed[4];
        }

        if (hasAnyText(
                normalizedOrganization,
                normalizedBusinessDomain,
                normalizedCapabilityDomain,
                normalizedApplication,
                normalizedRole)) {
            if (hasHierarchyGap(
                    normalizedOrganization,
                    normalizedBusinessDomain,
                    normalizedCapabilityDomain,
                    normalizedApplication,
                    normalizedRole)) {
                throw new IllegalArgumentException(
                        "service hierarchy filters must be contiguous from organization to role");
            }
            if (hasAllText(
                    normalizedOrganization,
                    normalizedBusinessDomain,
                    normalizedCapabilityDomain,
                    normalizedApplication,
                    normalizedRole)) {
                String canonicalService =
                        composeCanonicalServiceName(
                                normalizedOrganization,
                                normalizedBusinessDomain,
                                normalizedCapabilityDomain,
                                normalizedApplication,
                                normalizedRole);
                if (normalizedService != null && !normalizedService.equals(canonicalService)) {
                    throw new IllegalArgumentException(
                            "service must match structured service identity: " + canonicalService);
                }
                normalizedService = canonicalService;
                normalizedServices = appendUnique(normalizedServices, canonicalService);
            } else {
                String prefix =
                        composeHierarchyPrefix(
                                normalizedOrganization,
                                normalizedBusinessDomain,
                                normalizedCapabilityDomain,
                                normalizedApplication,
                                normalizedRole);
                normalizedPrefixes = appendUnique(normalizedPrefixes, prefix);
            }
        }

        return new ServiceFilterProjection(
                normalizedService,
                normalizedServices,
                normalizedPrefixes,
                normalizedOrganization,
                normalizedBusinessDomain,
                normalizedCapabilityDomain,
                normalizedApplication,
                normalizedRole);
    }

    private List<String> appendUnique(List<String> values, String value) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            return values;
        }
        List<String> result = values == null ? new ArrayList<>() : new ArrayList<>(values);
        if (!result.contains(normalized)) {
            result.add(normalized);
        }
        return result.isEmpty() ? null : List.copyOf(result);
    }

    private String composeCanonicalServiceName(
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role) {
        return String.join(
                ".",
                organization.trim(),
                businessDomain.trim(),
                capabilityDomain.trim(),
                application.trim(),
                role.trim());
    }

    private String composeHierarchyPrefix(
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role) {
        List<String> parts = new ArrayList<>(5);
        if (hasText(organization)) {
            parts.add(organization.trim());
        }
        if (hasText(businessDomain)) {
            parts.add(businessDomain.trim());
        }
        if (hasText(capabilityDomain)) {
            parts.add(capabilityDomain.trim());
        }
        if (hasText(application)) {
            parts.add(application.trim());
        }
        if (hasText(role)) {
            parts.add(role.trim());
        }
        return String.join(".", parts);
    }

    private boolean hasHierarchyGap(
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role) {
        boolean seenEmpty = false;
        for (String value :
                new String[] {organization, businessDomain, capabilityDomain, application, role}) {
            if (!hasText(value)) {
                seenEmpty = true;
                continue;
            }
            if (seenEmpty) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAnyText(String... values) {
        for (String value : values) {
            if (hasText(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAllText(String... values) {
        for (String value : values) {
            if (!hasText(value)) {
                return false;
            }
        }
        return values.length > 0;
    }

    private String[] parseCanonicalServiceName(String service) {
        String normalized = trimToNull(service);
        if (normalized == null) {
            return null;
        }
        String[] parts = normalized.split("\\.");
        if (parts.length != 5) {
            return null;
        }
        for (String part : parts) {
            if (!hasText(part)) {
                return null;
            }
        }
        return parts;
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private String requireText(String value, String fieldName) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private record ExactServiceIdentity(
            String service,
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role) {}

    private record ServiceFilterProjection(
            String service,
            List<String> services,
            List<String> servicePrefixes,
            String organization,
            String businessDomain,
            String capabilityDomain,
            String application,
            String role) {}
}
