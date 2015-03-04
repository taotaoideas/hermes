package com.ctrip.hermes.engine.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.initialization.Module;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesProducerModule;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.ConsumerPipeline;
import com.ctrip.hermes.engine.ConsumerTracingValve;
import com.ctrip.hermes.engine.DecodeMessageValve;
import com.ctrip.hermes.engine.LocalConsumerBootstrap;
import com.ctrip.hermes.engine.LocalConsumerValveRegistry;
import com.ctrip.hermes.engine.scanner.DefaultScanner;
import com.ctrip.hermes.engine.scanner.Scanner;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.message.codec.StoredMessageCodec;
import com.ctrip.hermes.spi.Valve;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	private static final String LOCAL_CONSUMER = "local-consumer";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(Module.class, HermesProducerModule.ID, HermesProducerModule.class));

		all.add(C(ValveRegistry.class, LOCAL_CONSUMER, LocalConsumerValveRegistry.class));
		all.add(C(Valve.class, DecodeMessageValve.ID, DecodeMessageValve.class) //
		      .req(CodecManager.class, StoredMessageCodec.class));
		all.add(C(Valve.class, ConsumerTracingValve.ID, ConsumerTracingValve.class));

		all.add(C(Pipeline.class, LOCAL_CONSUMER, ConsumerPipeline.class) //
		      .req(ValveRegistry.class, LOCAL_CONSUMER));

		all.add(C(ConsumerBootstrap.class, LocalConsumerBootstrap.ID, LocalConsumerBootstrap.class) //
		      .req(MessageChannelManager.class) //
		      .req(Pipeline.class, LOCAL_CONSUMER));

		all.add(C(Scanner.class, DefaultScanner.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
