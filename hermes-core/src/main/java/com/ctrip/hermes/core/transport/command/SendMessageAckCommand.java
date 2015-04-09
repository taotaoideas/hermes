package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageAckCommand extends AbstractCommand implements Ack {

	private Map<Integer, Boolean> m_successes = new ConcurrentHashMap<>();

	private int m_totalSize;

	public SendMessageAckCommand() {
		this(0);
	}

	public SendMessageAckCommand(int totalSize) {
		super(CommandType.ACK_MESSAGE_SEND);
		m_totalSize = totalSize;
	}

	public boolean isAllResultsSet() {
		return m_successes.size() == m_totalSize;
	}

	public void addResults(Map<Integer, Boolean> results) {
		m_successes.putAll(results);
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
		m_totalSize = codec.readInt();
		m_successes = codec.readMap();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeInt(m_totalSize);
		codec.writeMap(m_successes);
	}

}
