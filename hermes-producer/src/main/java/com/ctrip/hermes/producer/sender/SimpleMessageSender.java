package com.ctrip.hermes.producer.sender;

import java.util.concurrent.Future;

import com.ctrip.hermes.endpoint.EndpointChannel;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.ProducerMessage;
import com.ctrip.hermes.producer.api.SendResult;
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
