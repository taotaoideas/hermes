package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MessageAckCommand extends AbstractCommand {
	private ConcurrentMap<Tpp, Set<Long>> m_ackMsgSeqs = new ConcurrentHashMap<>();

	private ConcurrentMap<Tpp, Set<Long>> m_nackMsgSeqs = new ConcurrentHashMap<>();

	public MessageAckCommand() {
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
		m_ackMsgSeqs=readMsgSeqMap(codec);
		m_nackMsgSeqs=readMsgSeqMap(codec);
	}

	private void writeMsgSeqMap(HermesPrimitiveCodec codec, ConcurrentMap<Tpp, Set<Long>> msgMap) {
		if (msgMap == null) {
			codec.writeInt(0);
		} else {
			codec.writeInt(msgMap.size());
			for (Tpp tpp : msgMap.keySet()) {
				codec.writeString(tpp.getTopic());
				codec.writeInt(tpp.getPartition());
				codec.writeInt(tpp.isPriority() ? 0 : 1);
			}
			for (Tpp tpp : msgMap.keySet()) {
				Set<Long> msgSeqs = msgMap.get(tpp);
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

	private ConcurrentMap<Tpp, Set<Long>> readMsgSeqMap(HermesPrimitiveCodec codec) {
		ConcurrentMap<Tpp, Set<Long>> msgSeqMap = new ConcurrentHashMap<>();

		int mapSize = codec.readInt();
		if (mapSize != 0) {
			List<Tpp> tpps = new ArrayList<>();
			for (int i = 0; i < mapSize; i++) {
				Tpp tpp = new Tpp(codec.readString(), codec.readInt(), codec.readInt() == 0 ? true : false);
				tpps.add(tpp);
				msgSeqMap.put(tpp, new ConcurrentSkipListSet<Long>());
			}
			for (int i = 0; i < mapSize; i++) {
				int setSize = codec.readInt();
				Tpp tpp = tpps.get(i);
				for (int j = 0; j < setSize; j++) {
					msgSeqMap.get(tpp).add(codec.readLong());
				}
			}
		}

		return msgSeqMap;
	}

	public void addAckMsg(String topic, int partition, boolean priority, long msgSeq) {
		Tpp tpp = new Tpp(topic, partition, priority);
		m_ackMsgSeqs.putIfAbsent(tpp, new ConcurrentSkipListSet<Long>());
		m_ackMsgSeqs.get(tpp).add(msgSeq);
	}

	public void addNackMsg(String topic, int partition, boolean priority, long msgSeq) {
		Tpp tpp = new Tpp(topic, partition, priority);
		m_nackMsgSeqs.putIfAbsent(tpp, new ConcurrentSkipListSet<Long>());
		m_nackMsgSeqs.get(tpp).add(msgSeq);
	}

	public ConcurrentMap<Tpp, Set<Long>> getAckMsgs() {
		return m_ackMsgSeqs;
	}

	public ConcurrentMap<Tpp, Set<Long>> getNackMsgs() {
		return m_nackMsgSeqs;
	}

	public boolean isEmpty() {
		return m_nackMsgSeqs.isEmpty() && m_ackMsgSeqs.isEmpty();
	}

}
