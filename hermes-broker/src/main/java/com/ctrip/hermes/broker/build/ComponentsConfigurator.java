package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.broker.channel.BrokerDeliverValveRegistry;
import com.ctrip.hermes.broker.dal.HermesTableProvider;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityDao;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.MysqlQueueReader;
import com.ctrip.hermes.broker.queue.MysqlQueueWriter;
import com.ctrip.hermes.broker.queue.QueueReader;
import com.ctrip.hermes.broker.queue.QueueWriter;
import com.ctrip.hermes.broker.remoting.SendMessageRequestProcessor;
import com.ctrip.hermes.broker.remoting.SubscribeCommandProcessor;
import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.broker.remoting.netty.NettyServerConfig;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.meta.entity.Storage;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	public final static String BROKER = "broker";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(NettyServerConfig.class));
		all.add(C(NettyServer.class) //
		      .req(NettyServerConfig.class));

		all.add(C(MessageQueueManager.class, DefaultMessageQueueManager.ID, DefaultMessageQueueManager.class) //
		      .req(MetaService.class));

		all.add(C(ValveRegistry.class, BrokerDeliverValveRegistry.ID, BrokerDeliverValveRegistry.class));

		// processors
		all.add(C(CommandProcessor.class, SendMessageRequestProcessor.ID, SendMessageRequestProcessor.class) //
		      .req(MessageQueueManager.class, DefaultMessageQueueManager.ID));
		all.add(C(CommandProcessor.class, CommandType.SUBSCRIBE.toString(), SubscribeCommandProcessor.class) //
		      .req(MessageQueueManager.class, DefaultMessageQueueManager.ID));

		all.add(C(QueueWriter.class, Storage.MYSQL, MysqlQueueWriter.class) //
		      .req(MTopicPartitionPriorityDao.class));
		all.add(C(QueueReader.class, Storage.MYSQL, MysqlQueueReader.class) //
		      .req(MTopicPartitionPriorityDao.class));

		all.add(C(TableProvider.class, "m-topic-partition-priority", HermesTableProvider.class) //
		      .req(MetaService.class) //
		      .config(E("m_table").value("m-topic-partition-priority")));

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
