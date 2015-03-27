package com.ctrip.hermes.producer.sender;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.partition.PartitioningStrategy;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.producer.api.SendResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractMessageSender implements MessageSender {

	@Inject
	protected EndpointManager m_endpointManager;

	@Inject
	protected EndpointChannelManager m_endpointChannelManager;

	@Inject
	protected PartitioningStrategy m_partitioningAlgo;

	@Inject
	protected MetaService m_metaService;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.message.internal.MessageSender#send(com.ctrip.hermes.message.ProducerMessage)
	 */
	@Override
	public Future<SendResult> send(ProducerMessage<?> msg) {
		preSend(msg);
		return doSend(msg);
	}

	protected abstract Future<SendResult> doSend(ProducerMessage<?> msg);

	protected void preSend(ProducerMessage<?> msg) {
		int partitionNo = m_partitioningAlgo.computePartitionNo(msg.getPartition(),
		      m_metaService.getPartitions(msg.getTopic()).size());
		msg.setPartitionNo(partitionNo);
	}

}
