package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageAckCommand extends AbstractCommand implements Ack {

   public SendMessageAckCommand() {
	   super(CommandType.ACK_MESSAGE_SEND);
   }

	private Map<Integer, Boolean> m_successes = new HashMap<>();

	public void addResult(Integer msgSeq, boolean success) {
		m_successes.put(msgSeq, success);
	}

	public void addResults(List<Integer> msgSeqs, boolean success) {
		for (Integer seq : msgSeqs) {
			m_successes.put(seq, success);
		}
	}

	public boolean isSuccess(Integer msgSeqNo) {
		if (m_successes.containsKey(msgSeqNo)) {
			return m_successes.get(msgSeqNo);
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_successes = codec.readMap();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeMap(m_successes);
	}

}
