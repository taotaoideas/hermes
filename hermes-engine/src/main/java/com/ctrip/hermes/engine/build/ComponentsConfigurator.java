package com.ctrip.hermes.engine.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.engine.DecodeMessageValve;
import com.ctrip.hermes.engine.DefaultEngine;
import com.ctrip.hermes.engine.bootstrap.BrokerConsumerBootstrap;
import com.ctrip.hermes.engine.bootstrap.DefaultConsumerBootstrapManager;
import com.ctrip.hermes.engine.bootstrap.DefaultConsumerBootstrapRegistry;
import com.ctrip.hermes.engine.command.processor.ConsumeMessageCommandProcessor;
import com.ctrip.hermes.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.engine.notifier.DefaultConsumerNotifier;
import com.ctrip.hermes.engine.pipeline.ConsumerPipeline;
import com.ctrip.hermes.engine.pipeline.ConsumerValveRegistry;
import com.ctrip.hermes.engine.pipeline.internal.ConsumerTracingValve;
import com.ctrip.hermes.engine.scanner.DefaultScanner;
import com.ctrip.hermes.engine.scanner.Scanner;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// engine
		all.add(A(DefaultEngine.class));

		// bootstrap
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_CONSUME, ConsumeMessageCommandProcessor.class) //
				.req(ConsumerNotifier.class));

		all.add(A(DefaultConsumerBootstrapManager.class));
		all.add(A(DefaultConsumerBootstrapRegistry.class));
		all.add(A(BrokerConsumerBootstrap.class));
		
		// notifier
		all.add(A(DefaultConsumerNotifier.class));
		all.add(A(ConsumerValveRegistry.class));

		all.add(A(DecodeMessageValve.class));
		all.add(A(ConsumerTracingValve.class));

		all.add(A(ConsumerPipeline.class));

		all.add(C(Scanner.class, DefaultScanner.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
