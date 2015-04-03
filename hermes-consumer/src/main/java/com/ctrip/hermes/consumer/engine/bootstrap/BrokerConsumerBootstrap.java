package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.List;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.transport.command.SubscribeCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerBootstrap.class, value = Endpoint.BROKER)
public class BrokerConsumerBootstrap extends BaseConsumerBootstrap {

	@Override
	protected void doStart(ConsumerContext consumerContext) {

		List<Partition> partitions = consumerContext.getTopic().getPartitions();

		for (Partition partition : partitions) {

			Endpoint endpoint = m_endpointManager.getEndpoint(consumerContext.getTopic().getName(), partition.getId());
			EndpointChannel channel = m_endpointChannelManager.getChannel(endpoint);

			SubscribeCommand subscribeCommand = new SubscribeCommand();
			subscribeCommand.setGroupId(consumerContext.getGroupId());
			subscribeCommand.setTopic(consumerContext.getTopic().getName());
			subscribeCommand.setPartition(partition.getId());

			m_consumerNotifier.register(subscribeCommand.getHeader().getCorrelationId(), consumerContext);

			channel.writeCommand(subscribeCommand);
			// TODO handle ack , if success
		}

	}

}
