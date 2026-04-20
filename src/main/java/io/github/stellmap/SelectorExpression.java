package io.github.stellmap;

import java.util.Collection;
import java.util.stream.Collectors;

/** StarMap selector 表达式构造器。 */
public final class SelectorExpression {

    private SelectorExpression() {}

    /**
     * 构造存在性表达式，例如 version。
     *
     * @param key 标签 key
     * @return selector 表达式
     */
    public static String exists(String key) {
        return requireText(key, "selector key");
    }

    /**
     * 构造不存在表达式，例如 !deprecated。
     *
     * @param key 标签 key
     * @return selector 表达式
     */
    public static String notExists(String key) {
        return "!" + requireText(key, "selector key");
    }

    /**
     * 构造等值表达式，例如 color=gray。
     *
     * @param key 标签 key
     * @param value 标签 value
     * @return selector 表达式
     */
    public static String equalsTo(String key, String value) {
        return requireText(key, "selector key") + "=" + requireText(value, "selector value");
    }

    /**
     * 构造不等值表达式，例如 color!=gray。
     *
     * @param key 标签 key
     * @param value 标签 value
     * @return selector 表达式
     */
    public static String notEqualsTo(String key, String value) {
        return requireText(key, "selector key") + "!=" + requireText(value, "selector value");
    }

    /**
     * 构造 in 集合表达式，例如 version in (v1,v2)。
     *
     * @param key 标签 key
     * @param values 候选值
     * @return selector 表达式
     */
    public static String in(String key, Collection<String> values) {
        return requireText(key, "selector key") + " in " + formatSet(values);
    }

    /**
     * 构造 notin 集合表达式，例如 version notin (v1,v2)。
     *
     * @param key 标签 key
     * @param values 候选值
     * @return selector 表达式
     */
    public static String notIn(String key, Collection<String> values) {
        return requireText(key, "selector key") + " notin " + formatSet(values);
    }

    private static String formatSet(Collection<String> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("selector values must not be empty");
        }

        String joined =
                values.stream()
                        .map(value -> requireText(value, "selector value"))
                        .distinct()
                        .collect(Collectors.joining(","));
        if (joined.isBlank()) {
            throw new IllegalArgumentException("selector values must not be empty");
        }
        return "(" + joined + ")";
    }

    private static String requireText(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
