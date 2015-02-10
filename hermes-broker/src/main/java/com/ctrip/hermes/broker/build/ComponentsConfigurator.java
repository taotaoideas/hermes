package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.broker.DefaultMessageChannelManager;
import com.ctrip.hermes.broker.DefaultMessageQueueManager;
import com.ctrip.hermes.broker.MessageChannelManager;
import com.ctrip.hermes.broker.MessageQueueManager;
import com.ctrip.hermes.broker.remoting.HandshakeRequestProcessor;
import com.ctrip.hermes.broker.remoting.SendMessageRequestProcessor;
import com.ctrip.hermes.broker.remoting.StartConsumerRequestProcessor;
import com.ctrip.hermes.range.MyDefaultRangeMonitor;
import com.ctrip.hermes.range.RangeMonitor;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.netty.NettyServer;
import com.ctrip.hermes.remoting.netty.NettyServerConfig;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(NettyServerConfig.class));
		all.add(C(NettyServer.class) //
		      .req(NettyServerConfig.class));

		all.add(C(MessageChannelManager.class, DefaultMessageChannelManager.class) //
		      .req(MessageQueueManager.class));
		all.add(C(MessageQueueManager.class, DefaultMessageQueueManager.class));

		// processors
		all.add(C(CommandProcessor.class, HandshakeRequestProcessor.ID, HandshakeRequestProcessor.class));
		all.add(C(CommandProcessor.class, SendMessageRequestProcessor.ID, SendMessageRequestProcessor.class) //
		      .req(MessageChannelManager.class));
		all.add(C(CommandProcessor.class, StartConsumerRequestProcessor.ID, StartConsumerRequestProcessor.class) //
		      .req(MessageChannelManager.class));

		// rangeMonitor
//		all.add(C(RangeMonitor.class, MyDefaultRangeMonitor.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
