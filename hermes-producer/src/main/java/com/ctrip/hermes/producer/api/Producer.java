package com.ctrip.hermes.producer.api;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Producer {
	public abstract MessageHolder message(String topic, Object body);

	public static Producer getInstance() {
		return PlexusComponentLocator.lookup(Producer.class);
	}

	public interface MessageHolder {
		public MessageHolder withKey(String key);

		public Future<SendResult> send();

		public MessageHolder withPriority();

		public MessageHolder withPartition(String partition);

		public MessageHolder addProperty(String key, String value);
	}
}
