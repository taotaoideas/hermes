package com.ctrip.hermes.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.message.internal.DefaultMessagePipeline;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.internal.DefaultProducer;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(Producer.class, DefaultProducer.class) //
				.req(MessagePipeline.class));
		all.add(C(MessagePipeline.class, DefaultMessagePipeline.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
