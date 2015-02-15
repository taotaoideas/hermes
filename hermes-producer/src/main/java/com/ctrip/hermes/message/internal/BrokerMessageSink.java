package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandType;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;

public class BrokerMessageSink implements PipelineSink {
	public static final String ID = "broker";

	@Inject
	private ClientManager m_channelManager;

	public BrokerMessageSink() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handle(PipelineContext ctx, Object input) {
		Message<Object> msg = ctx.getSource();
		byte[] bodyBuf = (byte[]) input;
		String topic = msg.getTopic();

		NettyClientHandler client = m_channelManager.findProducerClient(topic);
		Command cmd = new Command(CommandType.SendMessageRequest) //
		      .setBody(bodyBuf) //
		      .addHeader("topic", topic);

		client.writeCommand(cmd);
	}
}
