package com.ctrip.hermes.storage.storage.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorageFactory {

	private Map<String, MemoryStorage<?>> m_storages = new ConcurrentHashMap<String, MemoryStorage<?>>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> MemoryStorage<T> findStorage(String id) {
		MemoryStorage<?> storage = m_storages.get(id);

		if (storage == null) {
			synchronized (this) {
				storage = m_storages.get(id);
				if (storage == null) {
					storage = new MemoryStorage(id);
					m_storages.put(id, storage);
				}
			}
		}

		return (MemoryStorage<T>) storage;
	}

}
