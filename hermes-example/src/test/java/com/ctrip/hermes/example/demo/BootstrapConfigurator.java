package com.ctrip.hermes.example.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.consumer.Consumer;

public class BootstrapConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(Consumer.class, uuid(), OrderNewConsumer1.class));
		all.add(C(Consumer.class, uuid(), OrderNewConsumer2A.class));
		all.add(C(Consumer.class, uuid(), OrderNewConsumer2B.class));
		all.add(C(Consumer.class, uuid(), OrderUpdateConsumer1.class));

		return all;
	}

	private String uuid() {
		return UUID.randomUUID().toString();
	}

	@Override
	protected Class<?> getTestClass() {
		return Bootstrap.class;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new BootstrapConfigurator());
	}

}
