package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.ProducerChannel;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.storage.message.Message;

public class SendMessageRequestProcessor implements CommandProcessor {

	public static final String ID = "send-message-request";

	@Inject
	private MessageChannelManager m_channelManager;

	@Inject
	private MessageCodec m_msgCodec;

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
		com.ctrip.hermes.message.Message<byte[]> pMsg = m_msgCodec.decode(body);
		// TODO
		Message msg = new Message();
		msg.setContent(pMsg.getBody());
		msg.setPartition(pMsg.getPartition());
		msg.setKey(pMsg.getKey());
		msg.setPriority(pMsg.isPriority() ? 0 : 1);

		return Arrays.asList(msg);
	}

}
