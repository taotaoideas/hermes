package com.ctrip.hermes.engine.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.initialization.Module;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesProducerModule;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.core.pipeline.spi.Valve;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.engine.DefaultEngine;
import com.ctrip.hermes.engine.Engine;
import com.ctrip.hermes.engine.bootstrap.BrokerConsumerBootstrap;
import com.ctrip.hermes.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.engine.bootstrap.ConsumerBootstrapManager;
import com.ctrip.hermes.engine.bootstrap.ConsumerBootstrapRegistry;
import com.ctrip.hermes.engine.bootstrap.DefaultConsumerBootstrapManager;
import com.ctrip.hermes.engine.bootstrap.DefaultConsumerBootstrapRegistry;
import com.ctrip.hermes.engine.bootstrap.KafkaConsumerBootstrap;
import com.ctrip.hermes.engine.command.processor.ConsumeMessageCommandProcessor;
import com.ctrip.hermes.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.engine.notifier.DefaultConsumerNotifier;
import com.ctrip.hermes.engine.pipeline.ConsumerPipeline;
import com.ctrip.hermes.engine.pipeline.ConsumerValveRegistry;
import com.ctrip.hermes.engine.pipeline.internal.ConsumerTracingValve;
import com.ctrip.hermes.meta.entity.Endpoint;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	private static final String CONSUMER = "consumer";

	private static final String LOCAL_CONSUMER = "local-consumer";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();
		all.add(C(Module.class, HermesProducerModule.ID, HermesProducerModule.class));

		// engine
		all.add(C(Engine.class, DefaultEngine.class) //
		      .req(ConsumerBootstrapManager.class)//
		      .req(MetaService.class)//
		);

		// bootstrap
		all.add(C(ConsumerBootstrapManager.class, DefaultConsumerBootstrapManager.class) //
		      .req(ConsumerBootstrapRegistry.class)//
		);
		all.add(C(ConsumerBootstrapRegistry.class, DefaultConsumerBootstrapRegistry.class));
		all.add(C(ConsumerBootstrap.class, Endpoint.BROKER, BrokerConsumerBootstrap.class)//
		      .req(EndpointChannelManager.class)//
		      .req(EndpointManager.class)//
		      .req(MetaService.class)//
		      .req(ConsumerNotifier.class)//
		);

		all.add(C(CommandProcessor.class, CommandType.MESSAGE_CONSUME, ConsumeMessageCommandProcessor.class));

		// notifier
		all.add(C(ConsumerNotifier.class, DefaultConsumerNotifier.class) //
		      .req(ConsumerPipeline.class)//
		);
		all.add(C(ConsumerPipeline.class) //
		      .req(ValveRegistry.class, CONSUMER));
		all.add(C(ValveRegistry.class, CONSUMER, ConsumerValveRegistry.class));

		all.add(C(Valve.class, ConsumerTracingValve.ID, ConsumerTracingValve.class));

		// Kafka
		all.add(C(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID, KafkaConsumerBootstrap.class) //
		      .req(ValveRegistry.class, LOCAL_CONSUMER) //
		      .req(Pipeline.class, LOCAL_CONSUMER));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
