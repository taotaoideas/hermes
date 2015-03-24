package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import com.ctrip.hermes.channel.EndpointChannel;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.remoting.command.SendMessageCommand;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SimpleMessageSender extends AbstractMessageSender implements MessageSender {

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {

		final SettableFuture<SendResult> future = SettableFuture.create();
		SendMessageCommand command = new SendMessageCommand();
		command.addMessage(msg, future);

		Endpoint endpoint = m_endpointManager.getEndpoint(msg.getTopic(), msg.getPartitionNo());
		EndpointChannel channel = m_endpointChannelManager.getChannel(endpoint);

		channel.write(command);
		return future;
	}
}
