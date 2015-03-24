package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.EndpointChannelManager;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.meta.MetaService;

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
	protected PartitioningAlgo m_partitioningAlgo;

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
