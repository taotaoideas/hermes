package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.MessageChannelManager;
import com.ctrip.hermes.broker.ProducerChannel;
import com.ctrip.hermes.broker.storage.message.Message;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class SendMessageRequestProcessor implements CommandProcessor {

	public static final String ID = "send-message-request";

	@Inject
	private MessageChannelManager m_channelManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.SendMessageRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();
		String topic = cmd.getHeader("topic");

		ProducerChannel channel = m_channelManager.newProducerChannel(topic);

		List<Message> msgs = decode(cmd.getBody());

		channel.send(msgs);
	}

	private List<Message> decode(byte[] body) {
		// TODO
		Message msg = new Message();
		msg.setContent(body);

		return Arrays.asList(msg);
	}

}
