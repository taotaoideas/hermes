package com.ctrip.hermes.message.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class BrokerMessageSink implements MessagePipelineSink {
	public static final String ID = "broker";

	@Inject
	private ClientManager m_channelManager;

	@Inject
	private CodecManager m_codecManager;

	public BrokerMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handle(PipelineContext<Message<Object>> ctx) {
		Message<Object> msg = ctx.getMessage();
		String topic = msg.getTopic();
		byte[] bodyBuf = encode(topic, Arrays.asList(msg));

		NettyClientHandler client = m_channelManager.findProducerClient(topic);
		Command cmd = new Command(CommandType.SendMessageRequest) //
		      .setBody(bodyBuf) //
		      .addHeader("topic", topic);

		client.writeCommand(cmd);
	}

	private byte[] encode(String topic, List<Message<Object>> msgs) {
		// TODO
		List<Object> bodies = Lists.transform(msgs, new Function<Message<Object>, Object>() {

			@Override
			public Object apply(Message<Object> msg) {
				return msg.getBody();
			}
		});

		return m_codecManager.getCodec(topic).encode(bodies);
	}
}
