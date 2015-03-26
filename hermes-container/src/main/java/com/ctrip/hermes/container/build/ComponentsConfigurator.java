package com.ctrip.hermes.container.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.container.BrokerConsumerBootstrap;
import com.ctrip.hermes.container.ConsumerValveRegistry;
import com.ctrip.hermes.container.KafkaConsumerBootstrap;
import com.ctrip.hermes.container.remoting.ConsumeRequestProcessor;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.ConsumerPipeline;
import com.ctrip.hermes.engine.DecodeMessageValve;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.message.codec.DefaultStoredMessageCodec;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.message.codec.StoredMessageCodec;
import com.ctrip.hermes.message.codec.internal.DefaultCodecManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.codec.ClientManager;
import com.ctrip.hermes.spi.Valve;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	private final static String BROKER_CONSUMER = "broker-consumer";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID, BrokerConsumerBootstrap.class) //
		      .req(ValveRegistry.class, BROKER_CONSUMER) //
		      .req(Pipeline.class, BROKER_CONSUMER) //
		      .req(ClientManager.class));
		all.add(C(ConsumerBootstrap.class, KafkaConsumerBootstrap.ID, KafkaConsumerBootstrap.class) //
		      .req(ValveRegistry.class, BROKER_CONSUMER) //
		      .req(Pipeline.class, BROKER_CONSUMER) //
		      .req(StoredMessageCodec.class) //
		      .req(MetaService.class)); //

		all.add(C(ValveRegistry.class, BROKER_CONSUMER, ConsumerValveRegistry.class));

		all.add(C(Pipeline.class, BROKER_CONSUMER, ConsumerPipeline.class) //
		      .req(ValveRegistry.class, BROKER_CONSUMER));

		all.add(C(CommandProcessor.class, ConsumeRequestProcessor.ID, ConsumeRequestProcessor.class) //
		      .req(ConsumerBootstrap.class, BrokerConsumerBootstrap.ID) //
		      .req(StoredMessageCodec.class));

		all.add(C(Valve.class, DecodeMessageValve.ID, DecodeMessageValve.class) //
		      .req(CodecManager.class));
		all.add(C(StoredMessageCodec.class, DefaultStoredMessageCodec.class));
		all.add(C(CodecManager.class, DefaultCodecManager.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
