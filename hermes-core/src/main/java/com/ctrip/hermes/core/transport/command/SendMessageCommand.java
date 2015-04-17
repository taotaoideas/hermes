package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctrip.hermes.core.ManualRelease;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@ManualRelease
public class SendMessageCommand extends AbstractCommand implements AckAware<SendMessageAckCommand> {

	/**
	 * msg counter within this command
	 */
	private AtomicInteger m_msgCounter = new AtomicInteger(0);

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

	public Map<Tpp, MessageRawDataBatch> getMessageRawDataBatches() {
		return m_decodedBatches;
	}

	public int getMessageCount() {
		return m_msgCounter.get();
	}

	@Override
	public void onAck(SendMessageAckCommand ack) {
		for (Map.Entry<Integer, SettableFuture<SendResult>> entry : m_futures.entrySet()) {
			entry.getValue().set(new SendResult(ack.isSuccess(entry.getKey())));
		}
	}

	@Override
	public void parse0(ByteBuf buf) {
		m_rawBuf = buf;

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_msgCounter.set(codec.readInt());

		List<Tpp> tppNames = readTppNames(buf);
		List<TppIndex> tppIndexes = readTppIndexes(buf, tppNames.size());

		readTpps(buf, codec, tppNames, tppIndexes);

	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeInt(m_msgCounter.get());

		writeTppNames(m_msgs, codec);

		buf.markWriterIndex();

		// placeholder for indexes
		for (int i = 0; i < m_msgs.size(); i++) {
			codec.writeInt(-1);
		}

		List<TppIndex> tppInfos = writeTpps(m_msgs.values(), codec, buf);

		int writerIndex = buf.writerIndex();
		buf.resetWriterIndex();

		writeTppIndexes(tppInfos, codec);

		buf.writerIndex(writerIndex);
	}

	private void writeTppIndexes(List<TppIndex> tppIndexes, HermesPrimitiveCodec codec) {
		for (TppIndex i : tppIndexes) {
			codec.writeInt(i.getLength());
		}
	}

	private List<TppIndex> readTppIndexes(ByteBuf buf, int size) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		List<TppIndex> tppInfos = new ArrayList<>(size);

		for (int i = 0; i < size; i++) {
			tppInfos.add(new TppIndex(codec.readInt()));
		}

		return tppInfos;
	}

	private void writeTppNames(Map<Tpp, List<ProducerMessage<?>>> msgs, HermesPrimitiveCodec codec) {
		codec.writeInt(msgs.size());

		for (Map.Entry<Tpp, List<ProducerMessage<?>>> entry : msgs.entrySet()) {
			Tpp tpp = entry.getKey();
			codec.writeString(tpp.getTopic());
			codec.writeInt(tpp.getPartition());
			codec.writeInt(tpp.isPriority() ? 0 : 1);
		}
	}

	private List<Tpp> readTppNames(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		int size = codec.readInt();

		List<Tpp> tppNames = new ArrayList<>();

		for (int i = 0; i < size; i++) {
			tppNames.add(new Tpp(codec.readString(), codec.readInt(), codec.readInt() == 0 ? true : false));
		}

		return tppNames;
	}

	private List<TppIndex> writeTpps(Collection<List<ProducerMessage<?>>> msgLists, HermesPrimitiveCodec codec,
	      ByteBuf buf) {
		List<TppIndex> tppIndexes = new ArrayList<>();

		for (List<ProducerMessage<?>> msgList : msgLists) {
			MessageCodec msgCodec = PlexusComponentLocator.lookup(MessageCodec.class);

			// write msgSeqs
			codec.writeInt(msgList.size());

			for (ProducerMessage<?> msg : msgList) {
				codec.writeInt(msg.getMsgSeqNo());
			}

			int start = buf.writerIndex();
			for (ProducerMessage<?> msg : msgList) {
				msgCodec.encode(msg, buf);
			}
			int length = buf.writerIndex() - start;
			tppIndexes.add(new TppIndex(length));
		}

		return tppIndexes;
	}

	private void readTpps(ByteBuf buf, HermesPrimitiveCodec codec, List<Tpp> tppNames, List<TppIndex> tppIndexes) {
		for (int i = 0; i < tppNames.size(); i++) {
			Tpp tppName = tppNames.get(i);
			TppIndex tppInfo = tppIndexes.get(i);

			int size = codec.readInt();
			List<Integer> msgSeqs = new ArrayList<>();

			for (int j = 0; j < size; j++) {
				msgSeqs.add(codec.readInt());
			}

			ByteBuf rawData = buf.readSlice(tppInfo.getLength());

			m_decodedBatches.put(tppName, new MessageRawDataBatch(tppName.getTopic(), msgSeqs, rawData));
		}
	}

	private static class TppIndex {

		private int m_length;

		public TppIndex(int length) {
			m_length = length;
		}

		public int getLength() {
			return m_length;
		}

	}

	public static class MessageRawDataBatch {
		private String m_topic;

		private List<Integer> m_msgSeqs;

		private ByteBuf m_rawData;

		private List<PartialDecodedMessage> m_msgs;

		public MessageRawDataBatch(String topic, List<Integer> msgSeqs, ByteBuf rawData) {
			m_topic = topic;
			m_msgSeqs = msgSeqs;
			m_rawData = rawData;
		}

		public String getTopic() {
			return m_topic;
		}

		public List<Integer> getMsgSeqs() {
			return m_msgSeqs;
		}

		public ByteBuf getRawData() {
			return m_rawData.duplicate();
		}

		public List<PartialDecodedMessage> getMessages() {

			if (m_msgs == null) {
				synchronized (this) {
					if (m_msgs == null) {
						m_msgs = new ArrayList<>();

						ByteBuf tmpBuf = m_rawData.duplicate();
						MessageCodec messageCodec = PlexusComponentLocator.lookup(MessageCodec.class);

						while (tmpBuf.readableBytes() > 0) {
							m_msgs.add(messageCodec.partialDecode(tmpBuf));
						}

					}
				}
			}

			return m_msgs;
		}

		public void release() {
			m_rawData.release();
		}
	}

}
