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
	private Map<Triple<String, Integer, Boolean>, List<ProducerMessage<?>>> m_msgs = new HashMap<>();

	/**
	 * msgSeq-future mapping
	 */
	private Map<Integer, SettableFuture<SendResult>> m_futures = new HashMap<>();

	private Map<Triple<String, Integer, Boolean>, MessageRawDataBatch> m_decodedBatches = new HashMap<>();

	/**
	 * @param commandType
	 */
	public SendMessageCommand() {
		super(CommandType.MESSAGE_SEND);
	}

	public void addMessage(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
		int msgSeqNo = m_msgCounter.getAndIncrement();
		msg.setMsgSeqNo(msgSeqNo);

		Triple<String, Integer, Boolean> key = new Triple<String, Integer, Boolean>(msg.getTopic(), msg.getPartitionNo(),
		      msg.isPriority());

		if (!m_msgs.containsKey(key)) {
			m_msgs.put(key, new ArrayList<ProducerMessage<?>>());
		}

		m_msgs.get(key).add(msg);

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
		List<Triple<String, Integer, Boolean>> tppNames = readTppNames(buf);
		List<TppInfo> tppInfos = readTppInfos(buf, tppNames.size());

		for (int i = 0; i < tppNames.size(); i++) {
			Triple<String, Integer, Boolean> tppName = tppNames.get(i);
			TppInfo tppInfo = tppInfos.get(i);

			buf.readerIndex(tppInfo.getStart());

			int size = codec.readInt();
			List<Integer> msgSeqs = new ArrayList<>();

			for (int j = 0; j < size; j++) {
				msgSeqs.add(codec.readInt());
			}

			ByteBuf rawData = buf.slice(buf.readerIndex(), tppInfo.getEnd() - buf.readerIndex() + 1);

			m_decodedBatches.put(tppName, new MessageRawDataBatch(tppName.getFirst(), size, msgSeqs, rawData));
		}

	}

	private List<TppInfo> readTppInfos(ByteBuf buf, int size) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		List<TppInfo> tppInfos = new ArrayList<>(size);

		for (int i = 0; i < size; i++) {
			tppInfos.add(new TppInfo(codec.readInt(), codec.readInt()));
		}

		return tppInfos;
	}

	private List<Triple<String, Integer, Boolean>> readTppNames(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		int size = codec.readInt();

		List<Triple<String, Integer, Boolean>> tppNames = new ArrayList<>();

		for (int i = 0; i < size; i++) {
			tppNames.add(new Triple<>(codec.readString(), codec.readInt(), codec.readBoolean()));
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

		List<TppInfo> tppInfos = writeTpps(m_msgs, buf);

		int writerIndex = buf.writerIndex();
		buf.resetWriterIndex();

		writeTppInfos(tppInfos, buf);

		buf.writerIndex(writerIndex);
	}

	private void writeTppInfos(List<TppInfo> tppInfos, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		for (TppInfo i : tppInfos) {
			codec.writeInt(i.getStart());
			codec.writeInt(i.getEnd());
		}
	}

	private void writeTppNames(Map<Triple<String, Integer, Boolean>, List<ProducerMessage<?>>> msgs, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeInt(msgs.size());

		for (Map.Entry<Triple<String, Integer, Boolean>, List<ProducerMessage<?>>> entry : msgs.entrySet()) {
			Triple<String, Integer, Boolean> key = entry.getKey();
			codec.writeString(key.getFirst());
			codec.writeInt(key.getMiddle());
			codec.writeBoolean(key.getLast());
		}
	}

	private List<TppInfo> writeTpps(Map<Triple<String, Integer, Boolean>, List<ProducerMessage<?>>> msgs, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		List<TppInfo> tppInfos = new ArrayList<>();

		for (Map.Entry<Triple<String, Integer, Boolean>, List<ProducerMessage<?>>> entry : msgs.entrySet()) {
			Triple<String, Integer, Boolean> key = entry.getKey();
			MessageCodec msgCodec = MessageCodecFactory.getMessageCodec(key.getFirst());

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
			tppInfos.add(new TppInfo(start, end));
		}

		return tppInfos;
	}

	public Map<Triple<String, Integer, Boolean>, MessageRawDataBatch> getMessageRawDataBatches() {
		return m_decodedBatches;
	}

	public static class TppInfo {

		private int m_start;

		private int m_end;

		public TppInfo(int start, int end) {
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
