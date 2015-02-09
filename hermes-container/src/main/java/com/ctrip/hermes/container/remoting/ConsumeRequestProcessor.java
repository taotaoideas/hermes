package com.ctrip.hermes.container.remoting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.engine.ConsumerManager;
import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandType;

public class ConsumeRequestProcessor implements CommandProcessor {

	public static final String ID = "sonsume-request";

	@Inject
	private ConsumerManager m_consumerManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ConsumeRequest);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();
		List<Message<?>> msgs = decode(cmd.getBody());

		m_consumerManager.deliverMessage(cmd.getCorrelationId(), msgs);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<Message<?>> decode(byte[] body) {
		Message msg = new Message();
		// TODO
		msg.setBody(new String(body));

		List<Message<?>> msgs = new ArrayList<>();
		msgs.add(msg);

		return msgs;
	}

}
