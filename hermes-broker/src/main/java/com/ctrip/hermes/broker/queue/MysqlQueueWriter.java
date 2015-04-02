package com.ctrip.hermes.broker.queue;

import io.netty.buffer.ByteBuf;

import java.util.Date;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriorityDao;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;

public class MysqlQueueWriter implements QueueWriter {
	@Inject
	private MTopicShardPriorityDao m_dao;

	@Override
	public void write(Tpp tpp, MessageRawDataBatch batch) throws StorageException {
		List<PartialDecodedMessage> messages = batch.getMessages();
		for (PartialDecodedMessage msg : messages) {
			MTopicShardPriority r = new MTopicShardPriority();
			r.setAttributes(new String(readByteBuf(msg.getAppProperties())));
			r.setCreationDate(new Date(msg.getBornTime()));
			r.setPayload(msg.readBody());
			r.setProducerId(1);
			r.setProducerIp("1.1.1.1");
			r.setRefKey(msg.getKey());

			r.setTopic(tpp.getTopic());
			r.setShard(tpp.getPartitionNo());
			r.setPriority(tpp.isPriority() ? 0 : 1);

			try {
				m_dao.insert(r);
			} catch (DalException e) {
				throw new StorageException("", e);
			}
		}
	}

	private byte[] readByteBuf(ByteBuf buf) {
		byte[] dst = new byte[buf.readableBytes()];
		buf.readBytes(dst);
		return dst;
	}

}
