package com.ctrip.hermes.storage.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CollectionUtil {

    @SuppressWarnings("unchecked")
    public static <T> List<T> toSafeList(List<T> list) {
        return list == null ? Collections.EMPTY_LIST : list;
    }

    public static boolean empty(Collection<?> c) {
        return c == null || c.size() == 0;
    }

    public static boolean notEmpty(Collection<?> c) {
        return c != null && c.size() > 0;
    }

    public static <T> T first(List<T> list) {
        T result = null;

        if (notEmpty(list)) {
            result = list.get(0);
        }

        return result;
    }

    public static <T> T last(List<T> list) {
        T result = null;

        if (notEmpty(list)) {
            result = list.get(list.size() - 1);
        }

        return result;
    }

}
