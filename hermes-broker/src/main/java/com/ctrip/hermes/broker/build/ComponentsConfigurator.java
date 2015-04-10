package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.broker.bootstrap.DefaultBrokerBootstrap;
import com.ctrip.hermes.broker.dal.HermesJdbcDataSourceDescriptorManager;
import com.ctrip.hermes.broker.dal.HermesTableProvider;
import com.ctrip.hermes.broker.dal.service.MessageService;
import com.ctrip.hermes.broker.dal.service.ResendService;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager;
import com.ctrip.hermes.broker.queue.DefaultMessageQueuePullerManager;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.MessageQueuePullerManager;
import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.broker.transport.NettyServerConfig;
import com.ctrip.hermes.broker.transport.command.processor.SendMessageCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.SubscribeCommandProcessor;
import com.ctrip.hermes.broker.transport.transmitter.DefaultMessageTransmitter;
import com.ctrip.hermes.broker.transport.transmitter.MessageTransmitter;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	public final static String BROKER = "broker";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultBrokerBootstrap.class));
		all.add(A(NettyServer.class));
		all.add(A(NettyServerConfig.class));

		all.add(C(CommandProcessor.class, CommandType.MESSAGE_SEND.toString(), SendMessageCommandProcessor.class)//
		      .req(MessageQueueManager.class));
		all.add(C(CommandProcessor.class, CommandType.SUBSCRIBE.toString(), SubscribeCommandProcessor.class)//
		      .req(MessageQueueManager.class)//
		      .req(MessageQueuePullerManager.class)//
		      .req(MessageTransmitter.class));

		all.add(A(DefaultMessageQueueManager.class));
		all.add(A(DefaultMessageQueuePullerManager.class));
		all.add(A(MessageService.class));
		all.add(A(ResendService.class));
		all.add(A(DefaultMessageTransmitter.class));

		all.add(C(TableProvider.class, "message-priority", HermesTableProvider.class) //
		      .req(MetaService.class) //
		      .config(E("m_table").value("message-priority")));
		all.add(C(TableProvider.class, "resend-group-id", HermesTableProvider.class) //
		      .req(MetaService.class) //
		      .config(E("m_table").value("resend-group-id")));
		all.add(C(TableProvider.class, "offset-message", HermesTableProvider.class) //
		      .req(MetaService.class) //
		      .config(E("m_table").value("offset-message")));
		all.add(C(TableProvider.class, "dead-letter", HermesTableProvider.class) //
				.req(MetaService.class) //
				.config(E("m_table").value("dead-letter")));

		all.add(A(HermesJdbcDataSourceDescriptorManager.class));

		all.addAll(new HermesDatabaseConfigurator().defineComponents());

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
