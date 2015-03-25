package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;
import com.ctrip.hermes.message.codec.HermesPrimitiveCodec;
import com.ctrip.hermes.message.codec.MessageCodec;
import com.ctrip.hermes.message.codec.MessageCodecFactory;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommand extends AbstractCommand implements AckAware<SendMessageAckCommand> {

	/**
	 * msg counter within this command
	 */
	private AtomicInteger m_msgCounter = new AtomicInteger(0);

	/**
	 * <pre>
	 * key:   topic-partitionNo-priority triple 
	 * value: list of msgSeq-msg pair
	 * </pre>
	 */
	private Map<Tpp, List<ProducerMessage<?>>> m_msgs = new HashMap<>();

	/**
	 * msgSeq-future mapping
	 */
	private Map<Integer, SettableFuture<SendResult>> m_futures = new HashMap<>();

	private Map<Tpp, MessageRawDataBatch> m_decodedBatches = new HashMap<>();

	/**
	 * @param commandType
	 */
	public SendMessageCommand() {
		super(CommandType.MESSAGE_SEND);
	}

	public void addMessage(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
		int msgSeqNo = m_msgCounter.getAndIncrement();
		msg.setMsgSeqNo(msgSeqNo);

		Tpp tpp = new Tpp(msg.getTopic(), msg.getPartitionNo(), msg.isPriority());

		if (!m_msgs.containsKey(tpp)) {
			m_msgs.put(tpp, new ArrayList<ProducerMessage<?>>());
		}

		m_msgs.get(tpp).add(msg);

		m_futures.put(msgSeqNo, future);
	}

	@Override
	public void onAck(SendMessageAckCommand ack) {
		for (Map.Entry<Integer, SettableFuture<SendResult>> entry : m_futures.entrySet()) {
			entry.getValue().set(new SendResult(ack.isSuccess(entry.getKey())));
		}
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		List<Tpp> tppNames = readTppNames(buf);
		List<TppIndex> tppIndexes = readTppInfos(buf, tppNames.size());

		for (int i = 0; i < tppNames.size(); i++) {
			Tpp tppName = tppNames.get(i);
			TppIndex tppInfo = tppIndexes.get(i);

			buf.readerIndex(tppInfo.getStart());

			int size = codec.readInt();
			List<Integer> msgSeqs = new ArrayList<>();

			for (int j = 0; j < size; j++) {
				msgSeqs.add(codec.readInt());
			}

			ByteBuf rawData = buf.slice(buf.readerIndex(), tppInfo.getEnd() - buf.readerIndex() + 1);

			m_decodedBatches.put(tppName, new MessageRawDataBatch(tppName.getTopic(), size, msgSeqs, rawData));
		}

	}

	private List<TppIndex> readTppInfos(ByteBuf buf, int size) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		List<TppIndex> tppInfos = new ArrayList<>(size);

		for (int i = 0; i < size; i++) {
			tppInfos.add(new TppIndex(codec.readInt(), codec.readInt()));
		}

		return tppInfos;
	}

	private List<Tpp> readTppNames(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		int size = codec.readInt();

		List<Tpp> tppNames = new ArrayList<>();

		for (int i = 0; i < size; i++) {
			tppNames.add(new Tpp(codec.readString(), codec.readInt(), codec.readBoolean()));
		}

		return tppNames;
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		writeTppNames(m_msgs, buf);

		buf.markWriterIndex();
		int tppInfoSize = m_msgs.size() * 10;
		// TODO fast skip
		buf.writeBytes(new byte[tppInfoSize]);

		List<TppIndex> tppInfos = writeTpps(m_msgs, buf);

		int writerIndex = buf.writerIndex();
		buf.resetWriterIndex();

		writeTppInfos(tppInfos, buf);

		buf.writerIndex(writerIndex);
	}

	private void writeTppInfos(List<TppIndex> tppInfos, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		for (TppIndex i : tppInfos) {
			codec.writeInt(i.getStart());
			codec.writeInt(i.getEnd());
		}
	}

	private void writeTppNames(Map<Tpp, List<ProducerMessage<?>>> msgs, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeInt(msgs.size());

		for (Map.Entry<Tpp, List<ProducerMessage<?>>> entry : msgs.entrySet()) {
			Tpp tpp = entry.getKey();
			codec.writeString(tpp.getTopic());
			codec.writeInt(tpp.getPartitionNo());
			codec.writeBoolean(tpp.isPriority());
		}
	}

	private List<TppIndex> writeTpps(Map<Tpp, List<ProducerMessage<?>>> msgs, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		List<TppIndex> tppIndexes = new ArrayList<>();

		for (Map.Entry<Tpp, List<ProducerMessage<?>>> entry : msgs.entrySet()) {
			Tpp tpp = entry.getKey();
			MessageCodec msgCodec = MessageCodecFactory.getMessageCodec(tpp.getTopic());

			int start = buf.writerIndex();

			// write msgSeqs
			codec.writeInt(entry.getValue().size());
			for (ProducerMessage<?> msg : entry.getValue()) {
				codec.writeInt(msg.getMsgSeqNo());
			}

			for (ProducerMessage<?> msg : entry.getValue()) {
				msgCodec.encode(msg, buf);
			}
			int end = buf.writerIndex() - 1;
			tppIndexes.add(new TppIndex(start, end));
		}

		return tppIndexes;
	}

	public Map<Tpp, MessageRawDataBatch> getMessageRawDataBatches() {
		return m_decodedBatches;
	}

	public static class Tpp {
		private Triple<String, Integer, Boolean> m_triple = new Triple<>();

		public Tpp(String topic, int partitionNo, boolean isPriority) {
			m_triple.setFirst(topic);
			m_triple.setMiddle(partitionNo);
			m_triple.setLast(isPriority);
		}

		public String getTopic() {
			return m_triple.getFirst();
		}

		public int getPartitionNo() {
			return m_triple.getMiddle();
		}

		public boolean isPriority() {
			return m_triple.getLast();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_triple == null) ? 0 : m_triple.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Tpp other = (Tpp) obj;
			if (m_triple == null) {
				if (other.m_triple != null)
					return false;
			} else if (!m_triple.equals(other.m_triple))
				return false;
			return true;
		}

	}

	private static class TppIndex {

		private int m_start;

		private int m_end;

		public TppIndex(int start, int end) {
			m_start = start;
			m_end = end;
		}

		public int getStart() {
			return m_start;
		}

		public int getEnd() {
			return m_end;
		}

	}

	public static class MessageRawDataBatch {
		private String m_topic;

		private int m_size;

		private List<Integer> m_msgSeqs;

		private ByteBuf m_rawData;

		private List<ProducerMessage<?>> m_msgs;

		public MessageRawDataBatch(String topic, int size, List<Integer> msgSeqs, ByteBuf rawData) {
			m_topic = topic;
			m_size = size;
			m_msgSeqs = msgSeqs;
			m_rawData = rawData;

		}

		public List<Integer> getMsgSeqs() {
			return m_msgSeqs;
		}

		public ByteBuf getRawData() {
			return m_rawData.duplicate();
		}

		public List<ProducerMessage<?>> getMessages() {
			if (m_msgs == null) {
				MessageCodec msgCodec = MessageCodecFactory.getMessageCodec(m_topic);
				m_msgs = new ArrayList<>(m_size);
				ByteBuf tmpRawData = m_rawData.duplicate();
				for (int i = 0; i < m_size; i++) {
					m_msgs.add(msgCodec.decode(tmpRawData));
				}
			}

			return m_msgs;
		}
	}

}
