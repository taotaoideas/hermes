package com.ctrip.hermes.broker.remoting;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.remoting.command.Command;
import com.ctrip.hermes.remoting.command.CommandContext;
import com.ctrip.hermes.remoting.command.CommandProcessor;
import com.ctrip.hermes.remoting.command.CommandType;
import com.ctrip.hermes.remoting.command.Header;
import com.ctrip.hermes.remoting.command.SendMessageAckCommand;

public class SendMessageRequestProcessor implements CommandProcessor {

	public static final String ID = "send-message-request";

	@Inject
	private MessageChannelManager m_channelManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND);
	}

	@Override
	public void process(CommandContext ctx) {
		Command req = ctx.getCommand();
		Header header = req.getHeader();

		List<SendResult> results = Collections.emptyList();

		SendMessageAckCommand ack = new SendMessageAckCommand();
		ack.correlate(req);
		ack.setSendResult(results);

		ctx.write(ack);

	}

}
