package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.broker.channel.BrokerDeliverValveRegistry;
import com.ctrip.hermes.broker.channel.BrokerMessageChannelManager;
import com.ctrip.hermes.broker.channel.BrokerMessageQueueManager;
import com.ctrip.hermes.broker.channel.BrokerReceiverValveRegistry;
import com.ctrip.hermes.broker.remoting.AckRequestProcessor;
import com.ctrip.hermes.broker.remoting.HandshakeRequestProcessor;
import com.ctrip.hermes.broker.remoting.SendMessageRequestProcessor;
import com.ctrip.hermes.broker.remoting.StartConsumerRequestProcessor;
import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.broker.remoting.netty.NettyServerConfig;
import com.ctrip.hermes.broker.remoting.netty.NettyServerHandler;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.MessageQueueManager;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.message.codec.StoredMessageCodec;
import com.ctrip.hermes.message.internal.DeliverPipeline;
import com.ctrip.hermes.message.internal.ReceiverPipeline;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandProcessorManager;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	public final static String BROKER = "broker";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(NettyServerConfig.class));
		all.add(C(NettyServer.class) //
		      .req(NettyServerConfig.class));

		all.add(C(NettyServerHandler.class).is(PER_LOOKUP) //
		      .req(CommandProcessorManager.class));

		all.add(C(MessageChannelManager.class, BrokerMessageChannelManager.ID, BrokerMessageChannelManager.class) //
		      .req(MessageQueueManager.class, BrokerMessageQueueManager.ID) //
		      .req(DeliverPipeline.class, BROKER) //
		      .req(ReceiverPipeline.class, BROKER));
		all.add(C(MessageQueueManager.class, BrokerMessageQueueManager.ID, BrokerMessageQueueManager.class) //
		      .req(MetaService.class));

		all.add(C(ValveRegistry.class, BrokerDeliverValveRegistry.ID, BrokerDeliverValveRegistry.class));
		all.add(C(DeliverPipeline.class, BROKER, DeliverPipeline.class) //
		      .req(ValveRegistry.class, BrokerDeliverValveRegistry.ID));
		all.add(C(ValveRegistry.class, BrokerReceiverValveRegistry.ID, BrokerReceiverValveRegistry.class));
		all.add(C(ReceiverPipeline.class, BROKER, ReceiverPipeline.class) //
		      .req(ValveRegistry.class, BrokerReceiverValveRegistry.ID));

		// processors
		all.add(C(CommandProcessor.class, HandshakeRequestProcessor.ID, HandshakeRequestProcessor.class));
		all.add(C(CommandProcessor.class, SendMessageRequestProcessor.ID, SendMessageRequestProcessor.class) //
		      .req(MessageChannelManager.class, BrokerMessageChannelManager.ID) //
		      .req(MessageCodec.class));
		all.add(C(CommandProcessor.class, StartConsumerRequestProcessor.ID, StartConsumerRequestProcessor.class) //
		      .req(MessageChannelManager.class, BrokerMessageChannelManager.ID) //
		      .req(StoredMessageCodec.class));
		all.add(C(CommandProcessor.class, AckRequestProcessor.ID, AckRequestProcessor.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
