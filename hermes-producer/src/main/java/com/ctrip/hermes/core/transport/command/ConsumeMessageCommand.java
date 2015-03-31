package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ConsumeMessageCommand extends AbstractCommand {

	private Map<Long, List<ConsumerMessageBatch>> m_msgs = new HashMap<>();

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

		for (Map.Entry<Long, List<ConsumerMessageBatch>> entry : m_msgs.entrySet()) {
			Long correlationId = entry.getKey();
			List<ConsumerMessageBatch> batchs = entry.getValue();
			writeBatchMetas(codec, batchs);

			writeBatchDatas(buf, codec, batchs);

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

		Map<Long, List<ConsumerMessageBatch>> msgs = new HashMap<>();

		for (int i = 0; i < correlationIdCount; i++) {
			long correlationId = correlationIds.get(i);
			List<ConsumerMessageBatch> batchs = new ArrayList<>();

			readBatchMetas(codec, batchs);

			readBatchDatas(buf, codec, batchs);

			msgs.put(correlationId, batchs);
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

	private void writeBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<ConsumerMessageBatch> batchs) {
		for (ConsumerMessageBatch batch : batchs) {
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

	private void readBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<ConsumerMessageBatch> batchs) {
		for (ConsumerMessageBatch batch : batchs) {
			int len = codec.readInt();
			batch.setData(buf.readSlice(len));
		}

	}

	private void readBatchMetas(HermesPrimitiveCodec codec, List<ConsumerMessageBatch> batchs) {
		int batchSize = codec.readInt();
		for (int i = 0; i < batchSize; i++) {
			ConsumerMessageBatch batch = new ConsumerMessageBatch();
			int seqSize = codec.readInt();
			batch.setTopic(codec.readString());

			for (int j = 0; j < seqSize; j++) {
				batch.addMsgSeq(codec.readLong());
			}
			batchs.add(batch);
		}
	}

	private void writeBatchMetas(HermesPrimitiveCodec codec, List<ConsumerMessageBatch> batchs) {
		codec.writeInt(batchs.size());
		for (ConsumerMessageBatch batch : batchs) {
			codec.writeInt(batch.getMsgSeqs().size());
			codec.writeString(batch.getTopic());
			for (Long seq : batch.getMsgSeqs()) {
				codec.writeLong(seq);
			}
		}
	}

	public void addMessage(long correlationId, ConsumerMessageBatch batch) {
		if (!m_msgs.containsKey(correlationId)) {
			m_msgs.put(correlationId, new ArrayList<ConsumerMessageBatch>());
		}

		m_msgs.get(correlationId).add(batch);
	}

	public Map<Long, List<ConsumerMessageBatch>> getMsgs() {
		return m_msgs;
	}

}
