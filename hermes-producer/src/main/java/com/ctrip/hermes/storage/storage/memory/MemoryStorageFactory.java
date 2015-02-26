package com.ctrip.hermes.storage.storage.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorageFactory {

	private Map<String, AbstractMemoryStorage<?>> m_storages = new ConcurrentHashMap<String, AbstractMemoryStorage<?>>();

	public MemoryOffsetStorage findOffsetStorage(String id) {
		return (MemoryOffsetStorage) findStorage(id, "offset");
	}

	public MemoryResendStorage findResendStorage(String id) {
		return (MemoryResendStorage) findStorage(id, "resend");

	}

	public MemoryMessageStorage findMessageStorage(String id) {
		return (MemoryMessageStorage) findStorage(id, "message");

	}

	private AbstractMemoryStorage<?> findStorage(String id, String type) {
		AbstractMemoryStorage<?> storage = m_storages.get(id);

		if (storage == null) {
			synchronized (this) {
				storage = m_storages.get(id);
				if (storage == null) {
					switch (type) {
					case "message":
						storage = new MemoryMessageStorage(id);
						break;
					case "resend":
						storage = new MemoryResendStorage(id);
						break;
					case "offset":
						storage = new MemoryOffsetStorage(id);
						break;

					default:
						throw new RuntimeException("Unknown memory storage type");
					}
					m_storages.put(id, storage);
				}
			}
		}

		return storage;
	}

}
