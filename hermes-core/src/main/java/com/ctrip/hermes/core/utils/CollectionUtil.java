package com.ctrip.hermes.core.utils;

import java.util.Collection;
import java.util.List;

public class CollectionUtil {

	public static <T> T last(List<T> list) {
		return isNullOrEmpty(list) ? null : list.get(list.size() - 1);
	}

	public static <T> T first(List<T> list) {
		return isNullOrEmpty(list) ? null : list.get(0);
	}

	public static boolean isNullOrEmpty(Collection<?> collection) {
		return collection == null || collection.isEmpty();
	}

}
