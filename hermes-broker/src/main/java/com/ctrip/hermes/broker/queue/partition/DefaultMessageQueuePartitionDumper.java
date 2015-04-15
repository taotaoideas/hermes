package com.ctrip.hermes.broker.queue.partition;

import java.util.Collection;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueuePartitionDumper extends AbstractMessageQueuePartitionDumper {

	private MessageQueueStorage m_storage;

	public DefaultMessageQueuePartitionDumper(String topic, int partition, MessageQueueStorage storage) {
		super(topic, partition);
		m_storage = storage;
	}

	@Override
	protected void doAppendMessageSync(boolean isPriority,
	      Collection<Pair<MessageRawDataBatch, Map<Integer, Boolean>>> todos) {

		try {
			// TODO m_storage is null if not lookup
			m_storage = PlexusComponentLocator.lookup(MessageQueueStorage.class, "mysql");
			m_storage.appendMessages(new Tpp(m_topic, m_partition, isPriority), Collections2.transform(todos,
			      new Function<Pair<MessageRawDataBatch, Map<Integer, Boolean>>, MessageRawDataBatch>() {

				      @Override
				      public MessageRawDataBatch apply(Pair<MessageRawDataBatch, Map<Integer, Boolean>> input) {
					      return input.getKey();
				      }
			      }));

			setBatchesResult(todos, true);
		} catch (Exception e) {
			setBatchesResult(todos, false);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void setBatchesResult(Collection<Pair<MessageRawDataBatch, Map<Integer, Boolean>>> todos, boolean success) {
		for (Pair<MessageRawDataBatch, Map<Integer, Boolean>> todo : todos) {
			Map<Integer, Boolean> result = todo.getValue();
			addResults(result, success);
		}
	}

}
