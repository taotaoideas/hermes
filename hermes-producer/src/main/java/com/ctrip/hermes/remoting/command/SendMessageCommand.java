package com.ctrip.hermes.remoting.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommand extends AbstractCommand implements AckAware<SendMessageAckCommand> {
	private AtomicInteger m_msgSeq = new AtomicInteger(0);

	private Map<Integer, ProducerMessage<?>> m_msgs = new HashMap<>();

	private Map<Integer, SettableFuture<SendResult>> m_futures = new HashMap<>();

	public void addMessage(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
		int msgSeqNo = m_msgSeq.getAndIncrement();
		m_msgs.put(msgSeqNo, msg);
		m_futures.put(msgSeqNo, future);
	}

	@Override
	public void onAck(SendMessageAckCommand ack) {
		for (Map.Entry<Integer, SettableFuture<SendResult>> entry : m_futures.entrySet()) {
			entry.getValue().set(new SendResult(ack.isSuccess(entry.getKey())));
		}
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
