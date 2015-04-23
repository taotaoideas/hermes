package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.ManualRelease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@ManualRelease
public class ConsumeMessageCommand extends AbstractCommand {

	private static final long serialVersionUID = 7746504198368995952L;

	private Map<Long, List<TppConsumerMessageBatch>> m_msgs = new HashMap<>();

	/**
	 * @param commandType
	 */
	public ConsumeMessageCommand() {
		super(CommandType.MESSAGE_CONSUME);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.transport.command.AbstractCommand#toBytes0(io.netty.buffer.ByteBuf)
	 */
	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		// size
		codec.writeInt(m_msgs.size());

		int indexBeforeHeader = buf.writerIndex();
		// placeholder for correlationIds
		for (int i = 0; i < m_msgs.size(); i++) {
			codec.writeLong(0);
		}

		// body
		List<Long> correlationIds = new ArrayList<>();

		for (Map.Entry<Long, List<TppConsumerMessageBatch>> entry : m_msgs.entrySet()) {
			Long correlationId = entry.getKey();
			List<TppConsumerMessageBatch> batches = entry.getValue();

			writeBatchMetas(codec, batches);
			writeBatchDatas(buf, codec, batches);

			correlationIds.add(correlationId);
		}
		int indexAfterBody = buf.writerIndex();

		buf.writerIndex(indexBeforeHeader);

		// header
		for (Long correlationId : correlationIds) {
			codec.writeLong(correlationId);
		}

		buf.writerIndex(indexAfterBody);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.core.transport.command.AbstractCommand#parse0(io.netty.buffer.ByteBuf)
	 */
	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		int correlationIdCount = codec.readInt();

		List<Long> correlationIds = readCorrelationIds(codec, correlationIdCount);

		Map<Long, List<TppConsumerMessageBatch>> msgs = new HashMap<>();

		for (int i = 0; i < correlationIdCount; i++) {
			long correlationId = correlationIds.get(i);
			List<TppConsumerMessageBatch> batches = new ArrayList<>();

			readBatchMetas(codec, batches);

			readBatchDatas(buf, codec, batches);

			msgs.put(correlationId, batches);
		}

		m_msgs = msgs;
	}

	private List<Long> readCorrelationIds(HermesPrimitiveCodec codec, int correlationIdCount) {
		List<Long> correlationIds = new ArrayList<>(correlationIdCount);
		for (int i = 0; i < correlationIdCount; i++) {
			correlationIds.add(codec.readLong());
		}
		return correlationIds;
	}

	private void writeBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		for (TppConsumerMessageBatch batch : batches) {
			// placeholder for len
			int start = buf.writerIndex();
			codec.writeInt(-1);
			int indexBeforeData = buf.writerIndex();
			batch.getTransferCallback().transfer(buf);
			int indexAfterData = buf.writerIndex();

			buf.writerIndex(start);
			codec.writeInt(indexAfterData - indexBeforeData);
			buf.writerIndex(indexAfterData);

		}
	}

	private void readBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		for (TppConsumerMessageBatch batch : batches) {
			int len = codec.readInt();
			batch.setData(buf.readSlice(len));
		}

	}

	private void readBatchMetas(HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		int batchSize = codec.readInt();
		for (int i = 0; i < batchSize; i++) {
			TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
			int seqSize = codec.readInt();
			batch.setTopic(codec.readString());
			batch.setPartition(codec.readInt());
			batch.setPriority(codec.readInt() == 0 ? true : false);
			batch.setResend(codec.readBoolean());

			for (int j = 0; j < seqSize; j++) {
				long msgSeq = codec.readLong();
				int remainingRetries = codec.readInt();
				batch.addMsgSeq(msgSeq, remainingRetries);
			}
			batches.add(batch);
		}
	}

	private void writeBatchMetas(HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		codec.writeInt(batches.size());
		for (TppConsumerMessageBatch batch : batches) {
			codec.writeInt(batch.size());
			codec.writeString(batch.getTopic());
			codec.writeInt(batch.getPartition());
			codec.writeInt(batch.isPriority() ? 0 : 1);
			codec.writeBoolean(batch.isResend());
			for (Pair<Long, Integer> pair : batch.getMsgSeqs()) {
				codec.writeLong(pair.getKey());
				codec.writeInt(pair.getValue());
			}
		}
	}

	public void addMessage(long correlationId, List<TppConsumerMessageBatch> batches) {
		if (!m_msgs.containsKey(correlationId)) {
			m_msgs.put(correlationId, new ArrayList<TppConsumerMessageBatch>());
		}

		m_msgs.get(correlationId).addAll(batches);
	}

	public Map<Long, List<TppConsumerMessageBatch>> getMsgs() {
		return m_msgs;
	}

}
