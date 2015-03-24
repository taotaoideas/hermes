package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.EndpointChannel;
import com.ctrip.hermes.channel.EndpointChannelManager;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.remoting.command.SendMessageCommand;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SimpleMessageSender implements MessageSender {

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private EndpointChannelManager m_endpointChannelManager;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.message.internal.MessageSender#send(com.ctrip.hermes.message.ProducerMessage)
	 */
	@Override
	public Future<SendResult> send(ProducerMessage<?> msg) {

		final SettableFuture<SendResult> future = SettableFuture.create();
		SendMessageCommand command = new SendMessageCommand();
		command.addMessage(msg, future);

		Endpoint endpoint = m_endpointManager.getEndpoint(msg.getTopic(), msg.getPartition());
		EndpointChannel channel = m_endpointChannelManager.getChannel(endpoint);

		channel.write(command);
		return future;
	}
}
