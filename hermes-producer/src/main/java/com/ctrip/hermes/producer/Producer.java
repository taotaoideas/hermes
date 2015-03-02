package com.ctrip.hermes.producer;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.unidal.lookup.ContainerLoader;

public abstract class Producer {
	public abstract Holder message(String topic, Object body);

	public static Producer getInstance() {
		try {
			return ContainerLoader.getDefaultContainer().lookup(Producer.class);
		} catch (ComponentLookupException e) {
			throw new IllegalStateException(String.format("Error: Unable to lookup %s!", Producer.class.getName()), e);
		}
	}

	public interface Holder {
		public Holder withKey(String key);

		public void send();

		public Holder withPriority();
	}
}
