package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.HandshakeRequestProcessor;
import com.ctrip.hermes.remoting.netty.NettyRemotingServer;
import com.ctrip.hermes.remoting.netty.NettyServerConfig;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(NettyServerConfig.class));
		all.add(C(NettyRemotingServer.class) //
		      .req(NettyServerConfig.class));

		all.add(C(CommandProcessor.class, HandshakeRequestProcessor.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
