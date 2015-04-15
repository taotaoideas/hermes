package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommand extends AbstractCommand {
	private ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> m_ackMsgSeqs = new ConcurrentHashMap<>();

	private ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> m_nackMsgSeqs = new ConcurrentHashMap<>();

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
	      ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> msgSeqMap) {
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
				Set<Long> msgSeqs = msgSeqMap.get(tppgr);
				if (msgSeqs == null) {
					codec.writeInt(0);
				} else {
					codec.writeInt(msgSeqs.size());
					for (Long msgSeq : msgSeqs) {
						codec.writeLong(msgSeq);
					}
				}
			}
		}

	}

	private ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> readMsgSeqMap(HermesPrimitiveCodec codec) {
		ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> msgSeqMap = new ConcurrentHashMap<>();

		int mapSize = codec.readInt();
		if (mapSize != 0) {
			List<Triple<Tpp, String, Boolean>> tppgrs = new ArrayList<>();
			for (int i = 0; i < mapSize; i++) {
				Tpp tpp = new Tpp(codec.readString(), codec.readInt(), codec.readInt() == 0 ? true : false);
				String groupId = codec.readString();
				boolean resend = codec.readBoolean();
				Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
				tppgrs.add(key);
				msgSeqMap.put(key, new ConcurrentSkipListSet<Long>());
			}
			for (int i = 0; i < mapSize; i++) {
				int setSize = codec.readInt();
				Triple<Tpp, String, Boolean> tppgr = tppgrs.get(i);
				for (int j = 0; j < setSize; j++) {
					msgSeqMap.get(tppgr).add(codec.readLong());
				}
			}
		}

		return msgSeqMap;
	}

	public void addAckMsg(String topic, int partition, boolean priority, String groupId, boolean resend, long msgSeq) {
		Tpp tpp = new Tpp(topic, partition, priority);
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		if (!m_ackMsgSeqs.containsKey(key)) {
			m_ackMsgSeqs.putIfAbsent(key, new ConcurrentSkipListSet<Long>());
		}
		m_ackMsgSeqs.get(key).add(msgSeq);
	}

	public void addNackMsg(String topic, int partition, boolean priority, String groupId, boolean resend, long msgSeq) {
		Tpp tpp = new Tpp(topic, partition, priority);
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		if (!m_nackMsgSeqs.containsKey(key)) {
			m_nackMsgSeqs.putIfAbsent(key, new ConcurrentSkipListSet<Long>());
		}
		m_nackMsgSeqs.get(key).add(msgSeq);
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> getAckMsgs() {
		return m_ackMsgSeqs;
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, Set<Long>> getNackMsgs() {
		return m_nackMsgSeqs;
	}

	public boolean isEmpty() {
		return m_nackMsgSeqs.isEmpty() && m_ackMsgSeqs.isEmpty();
	}

}
