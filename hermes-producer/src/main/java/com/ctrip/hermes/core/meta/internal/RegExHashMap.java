package com.ctrip.hermes.core.meta.internal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

public class RegExHashMap<String, V> extends HashMap<String, V> {
	public V put(String key, V value) {
		return super.put(key, value);
	}

	@Override
	public V get(Object key) {
		Iterator<String> regexps = keySet().iterator();
		String keyString;
		V result = null;

		while (regexps.hasNext()) {
			keyString = regexps.next();
			if (Pattern.matches(key.toString(), (CharSequence) keyString)) {
				result = super.get(keyString);
				break;
			}
		}
		return result;
	}

}
