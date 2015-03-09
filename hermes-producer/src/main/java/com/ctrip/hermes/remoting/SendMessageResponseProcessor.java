package com.ctrip.hermes.remoting;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.remoting.future.FutureManager;

public class SendMessageResponseProcessor implements CommandProcessor {

	public static final String ID = "send-message-response";

	@Inject
	private FutureManager m_futureManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.SendMessageResponse);
	}

	@Override
	public void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();

		List<SendResult> sendResult = JSON.parseObject(cmd.getBody(), new TypeReference<List<SendResult>>() {
		}.getType());

		// TODO will only send one message for now
		m_futureManager.futureDone(cmd.getCorrelationId(), sendResult.get(0));
	}

}
