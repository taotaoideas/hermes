package com.ctrip.hermes.broker.remoting;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.ProducerChannel;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

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
		List<ProducerMessage<byte[]>> msgs = decode(cmd.getBody());
		List<SendResult> results = channel.send(msgs);

		ctx.write(new Command(CommandType.SendMessageResponse) //
		      .setBody(JSON.toJSONBytes(results)) //
		      .setCorrelationId(cmd.getCorrelationId()));

	}

	private List<ProducerMessage<byte[]>> decode(byte[] body) {
		// TODO use netty bytebuffer
		ProducerMessage<byte[]> pMsg = m_msgCodec.decode(ByteBuffer.wrap(body));

		return Arrays.asList(pMsg);
	}

}
