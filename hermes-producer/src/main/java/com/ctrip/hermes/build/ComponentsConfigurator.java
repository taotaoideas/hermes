package com.ctrip.hermes.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.message.internal.DefaultMessagePipeline;
import com.ctrip.hermes.message.internal.DefaultMessageRegistry;
import com.ctrip.hermes.message.internal.DefaultMessageSinkManager;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.internal.DefaultProducer;
import com.ctrip.hermes.spi.MessageValve;
import com.ctrip.hermes.spi.internal.TracingMessageValve;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(Producer.class, DefaultProducer.class) //
		      .req(MessagePipeline.class));
		all.add(C(MessagePipeline.class, DefaultMessagePipeline.class) //
		      .req(MessageRegistry.class, MessageSinkManager.class));
		all.add(C(MessageSinkManager.class, DefaultMessageSinkManager.class));
		all.add(C(MessageRegistry.class, DefaultMessageRegistry.class));

		all.add(C(MessageValve.class, TracingMessageValve.ID, TracingMessageValve.class));
		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
