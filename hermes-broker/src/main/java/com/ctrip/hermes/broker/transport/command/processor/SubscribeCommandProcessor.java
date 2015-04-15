package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.queue.partition.MessageQueuePartitionPullerManager;
import com.ctrip.hermes.broker.transport.transmitter.MessageTransmitter;
import com.ctrip.hermes.broker.transport.transmitter.TpgRelayer;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SubscribeCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

public class SubscribeCommandProcessor implements CommandProcessor {

	@Inject
	private MessageQueuePartitionPullerManager m_queuePartitionPullerManager;

	@Inject
	private MessageTransmitter m_transmitter;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.SUBSCRIBE);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		SubscribeCommand reqCmd = (SubscribeCommand) ctx.getCommand();
		// TODO validate topic, partition, check if this broker is the leader of the topic-partition

		long correlationId = reqCmd.getHeader().getCorrelationId();
		Tpg tpg = new Tpg(reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());
		TpgRelayer relayer = m_transmitter.registerDestination(tpg, correlationId, ctx.getChannel(), reqCmd.getWindow());
		m_queuePartitionPullerManager.startPuller(tpg, relayer);
	}

}
