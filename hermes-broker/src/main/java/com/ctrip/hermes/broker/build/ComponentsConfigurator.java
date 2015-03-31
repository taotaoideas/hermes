package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.broker.channel.BrokerDeliverValveRegistry;
import com.ctrip.hermes.broker.channel.BrokerMessageQueueManager;
import com.ctrip.hermes.broker.channel.BrokerReceiverValveRegistry;
import com.ctrip.hermes.broker.channel.MessageQueueManager;
import com.ctrip.hermes.broker.dal.HermesTableProvider;
import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriorityDao;
import com.ctrip.hermes.broker.remoting.SendMessageRequestProcessor;
import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.broker.remoting.netty.NettyServerConfig;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.message.internal.DeliverPipeline;
import com.ctrip.hermes.message.internal.ReceiverPipeline;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	public final static String BROKER = "broker";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(NettyServerConfig.class));
		all.add(C(NettyServer.class) //
		      .req(NettyServerConfig.class));

		all.add(C(MessageQueueManager.class, BrokerMessageQueueManager.ID, BrokerMessageQueueManager.class) //
		      .req(MetaService.class));

		all.add(C(ValveRegistry.class, BrokerDeliverValveRegistry.ID, BrokerDeliverValveRegistry.class));
		all.add(C(DeliverPipeline.class, BROKER, DeliverPipeline.class) //
		      .req(ValveRegistry.class, BrokerDeliverValveRegistry.ID));
		all.add(C(ValveRegistry.class, BrokerReceiverValveRegistry.ID, BrokerReceiverValveRegistry.class));
		all.add(C(ReceiverPipeline.class, BROKER, ReceiverPipeline.class) //
		      .req(ValveRegistry.class, BrokerReceiverValveRegistry.ID));

		// processors
		all.add(C(CommandProcessor.class, SendMessageRequestProcessor.ID, SendMessageRequestProcessor.class) //
		      .req(MessageQueueManager.class, BrokerMessageQueueManager.ID) //
		      .req(MTopicShardPriorityDao.class));

		all.add(C(TableProvider.class, "m-topic-shard-priority", HermesTableProvider.class) //
				.req(MetaService.class) //
		      .config(E("m_table").value("m-topic-shard-priority")));

		all.add(defineJdbcDataSourceConfigurationManagerComponent("/data/appdatas/hermes/datasources.xml"));

		all.addAll(new HermesDatabaseConfigurator().defineComponents());

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
