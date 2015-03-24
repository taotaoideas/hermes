package com.ctrip.hermes.remoting.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.channel.SendResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageAckCommand extends AbstractCommand implements Ack {

	private Map<Integer, Boolean> m_successes = new HashMap<>();

	private List<SendResult> m_sendResult;

	public boolean isSuccess(Integer msgSeqNo) {
		if (m_successes.containsKey(msgSeqNo)) {
			return m_successes.get(msgSeqNo);
		}
		return false;
	}

	public void setSendResult(List<SendResult> sendResult) {
		m_sendResult = sendResult;
	}

	@Override
	public void doParse(ByteBuffer buf) {
		// TODO Auto-generated method stub

	}

	@Override
	public ByteBuffer doToByteBuffer() {
		// TODO Auto-generated method stub
		return null;
	}

}
