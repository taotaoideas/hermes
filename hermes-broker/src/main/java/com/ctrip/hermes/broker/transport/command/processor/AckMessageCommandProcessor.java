package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.ack.AckManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.AckMessageCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommandProcessor implements CommandProcessor {

	@Inject
	private AckManager m_ackManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_ACK);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		AckMessageCommand cmd = (AckMessageCommand) ctx.getCommand();

		for (Map.Entry<Triple<Tpp, String, Boolean>, Map<Long, Integer>> entry : cmd.getAckMsgs().entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean resend = entry.getKey().getLast();
			Map<Long, Integer> msgSeqs = entry.getValue();
			m_ackManager.acked(tpp, groupId, resend, msgSeqs);
		}

		for (Map.Entry<Triple<Tpp, String, Boolean>, Map<Long, Integer>> entry : cmd.getNackMsgs().entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean resend = entry.getKey().getLast();
			Map<Long, Integer> msgSeqs = entry.getValue();
			m_ackManager.nacked(tpp, groupId, resend, msgSeqs);
		}
	}
}
