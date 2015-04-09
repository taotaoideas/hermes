package com.ctrip.hermes.consumer.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.consumer.engine.DefaultEngine;
import com.ctrip.hermes.consumer.engine.bootstrap.BrokerConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.DefaultConsumerBootstrapManager;
import com.ctrip.hermes.consumer.engine.bootstrap.DefaultConsumerBootstrapRegistry;
import com.ctrip.hermes.consumer.engine.command.processor.ConsumeMessageCommandProcessor;
import com.ctrip.hermes.consumer.engine.consumer.pipeline.internal.ConsumerTracingValve;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.consumer.engine.notifier.DefaultConsumerNotifier;
import com.ctrip.hermes.consumer.engine.pipeline.ConsumerPipeline;
import com.ctrip.hermes.consumer.engine.pipeline.ConsumerValveRegistry;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// engine
		all.add(A(DefaultEngine.class));

		// bootstrap
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_CONSUME.toString(), ConsumeMessageCommandProcessor.class) //
		      .req(ConsumerNotifier.class));

		all.add(A(DefaultConsumerBootstrapManager.class));
		all.add(A(DefaultConsumerBootstrapRegistry.class));
		all.add(A(BrokerConsumerBootstrap.class));

		// notifier
		all.add(A(DefaultConsumerNotifier.class));
		all.add(A(ConsumerValveRegistry.class));

		all.add(A(ConsumerTracingValve.class));

		all.add(A(ConsumerPipeline.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
