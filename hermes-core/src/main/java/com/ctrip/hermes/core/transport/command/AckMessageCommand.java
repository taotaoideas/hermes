package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommand extends AbstractCommand {
	// key: tpp, groupId, isResend
	// value:msgSeq==>remainingRetries
	private ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> m_ackMsgSeqs = new ConcurrentHashMap<>();

	// key: tpp, groupId, isResend
	// value:msgSeq==>remainingRetries
	private ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> m_nackMsgSeqs = new ConcurrentHashMap<>();

	public AckMessageCommand() {
		super(CommandType.MESSAGE_ACK);
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		writeMsgSeqMap(codec, m_ackMsgSeqs);
		writeMsgSeqMap(codec, m_nackMsgSeqs);

	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_ackMsgSeqs = readMsgSeqMap(codec);
		m_nackMsgSeqs = readMsgSeqMap(codec);
	}

	private void writeMsgSeqMap(HermesPrimitiveCodec codec,
	      ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> msgSeqMap) {
		if (msgSeqMap == null) {
			codec.writeInt(0);
		} else {
			codec.writeInt(msgSeqMap.size());
			for (Triple<Tpp, String, Boolean> tppgr : msgSeqMap.keySet()) {
				Tpp tpp = tppgr.getFirst();
				codec.writeString(tpp.getTopic());
				codec.writeInt(tpp.getPartition());
				codec.writeInt(tpp.isPriority() ? 0 : 1);
				codec.writeString(tppgr.getMiddle());
				codec.writeBoolean(tppgr.getLast());
			}
			for (Triple<Tpp, String, Boolean> tppgr : msgSeqMap.keySet()) {
				Map<Long, Integer> msgSeqs = msgSeqMap.get(tppgr);
				codec.writeMap(msgSeqs);
			}
		}

	}

	@SuppressWarnings("unchecked")
	private ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> readMsgSeqMap(HermesPrimitiveCodec codec) {
		ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> msgSeqMap = new ConcurrentHashMap<>();

		int mapSize = codec.readInt();
		if (mapSize != 0) {
			List<Triple<Tpp, String, Boolean>> tppgrs = new ArrayList<>();
			for (int i = 0; i < mapSize; i++) {
				Tpp tpp = new Tpp(codec.readString(), codec.readInt(), codec.readInt() == 0 ? true : false);
				String groupId = codec.readString();
				boolean resend = codec.readBoolean();
				tppgrs.add(new Triple<>(tpp, groupId, resend));
			}
			for (int i = 0; i < mapSize; i++) {
				Triple<Tpp, String, Boolean> tppgr = tppgrs.get(i);
				msgSeqMap.put(tppgr, codec.readMap());
			}
		}

		return msgSeqMap;
	}

	public void addAckMsg(Tpp tpp, String groupId, boolean resend, long msgSeq, int remainingRetries) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		if (!m_ackMsgSeqs.containsKey(key)) {
			m_ackMsgSeqs.putIfAbsent(key, new ConcurrentHashMap<Long, Integer>());
		}
		m_ackMsgSeqs.get(key).put(msgSeq, remainingRetries);
	}

	public void addNackMsg(Tpp tpp, String groupId, boolean resend, long msgSeq, int remainingRetries) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		if (!m_nackMsgSeqs.containsKey(key)) {
			m_nackMsgSeqs.putIfAbsent(key, new ConcurrentHashMap<Long, Integer>());
		}
		m_nackMsgSeqs.get(key).put(msgSeq, remainingRetries);
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> getAckMsgs() {
		return m_ackMsgSeqs;
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, Map<Long, Integer>> getNackMsgs() {
		return m_nackMsgSeqs;
	}

	public boolean isEmpty() {
		return m_nackMsgSeqs.isEmpty() && m_ackMsgSeqs.isEmpty();
	}

}
