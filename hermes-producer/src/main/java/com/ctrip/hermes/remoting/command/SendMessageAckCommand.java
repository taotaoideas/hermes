package com.ctrip.hermes.remoting.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageAckCommand extends AbstractCommand implements Ack {

	private Map<Integer, Boolean> m_successes = new HashMap<>();

	public boolean isSuccess(Integer msgSeqNo) {
		if (m_successes.containsKey(msgSeqNo)) {
			return m_successes.get(msgSeqNo);
		}
		return false;
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
