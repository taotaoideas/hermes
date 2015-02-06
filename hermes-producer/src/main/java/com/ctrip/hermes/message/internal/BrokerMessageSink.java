package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ProducerChannel;
import com.ctrip.hermes.remoting.netty.MessageChannelManager;

public class BrokerMessageSink implements MessageSink {
	public static final String ID = "broker";

	@Inject
	private MessageChannelManager m_channelManager;

	@Inject
	private CodecManager m_codecManager;

	public BrokerMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handle(MessageContext ctx) {
		Message<Object> msg = ctx.getMessage();
		String topic = msg.getTopic();
		byte[] bodyBuf = m_codecManager.getCodec(topic).encode(msg.getBody());

		ProducerChannel channel = m_channelManager.findProducerChannel(topic);
		Command cmd = new Command(CommandType.SendMessageRequest) //
		      .setBody(bodyBuf) //
		      .addHeader("topic", topic);;

		channel.writeCommand(cmd);
	}

}
