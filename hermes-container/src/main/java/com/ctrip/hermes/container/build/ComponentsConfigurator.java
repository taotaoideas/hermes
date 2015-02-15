package com.ctrip.hermes.container.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.container.ConsumerPipeline;
import com.ctrip.hermes.container.ConsumerValveRegistry;
import com.ctrip.hermes.container.DecodeMessageValve;
import com.ctrip.hermes.container.DefaultConsumerBootstrap;
import com.ctrip.hermes.container.remoting.ConsumeRequestProcessor;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.spi.Valve;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	private final static String CONSUMER = "consumer";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(ConsumerBootstrap.class, DefaultConsumerBootstrap.class) //
		      .req(ValveRegistry.class, CONSUMER) //
		      .req(Pipeline.class, CONSUMER));

		all.add(C(ValveRegistry.class, CONSUMER, ConsumerValveRegistry.class));
		all.add(C(Valve.class, DecodeMessageValve.ID, DecodeMessageValve.class));

		all.add(C(Pipeline.class, CONSUMER, ConsumerPipeline.class) //
		      .req(ValveRegistry.class, CONSUMER));

		all.add(C(CommandProcessor.class, ConsumeRequestProcessor.ID, ConsumeRequestProcessor.class) //
		      .req(ConsumerBootstrap.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
