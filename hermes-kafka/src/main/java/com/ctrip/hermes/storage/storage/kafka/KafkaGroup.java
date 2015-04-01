package com.ctrip.hermes.storage.storage.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.storage.pair.MessagePair;
import com.ctrip.hermes.storage.pair.ResendPair;
import com.ctrip.hermes.storage.pair.StoragePair;

public class KafkaGroup {

	private String m_topic;

	private Properties m_pc;

	private Properties m_cc;

	public KafkaGroup(String topic, Properties pc, Properties cc) {
		m_topic = topic;
		m_pc = pc;
		m_cc = cc;
	}

	public StoragePair<Record> createMessagePair() {
		KafkaMessageStorage main = new KafkaMessageStorage(m_topic, m_pc, m_cc);

		MessagePair pair = new MessagePair(main, null);

		ClusteredMessagePair cluster = new ClusteredMessagePair(Arrays.asList(pair));

		if (m_cc.containsKey("group.id")) {
			List<Pair<String, Integer>> brokers = new ArrayList<>();
			String[] brokerList = null;
			if (m_cc.containsKey("metadata.broker.list"))
				brokerList = m_cc.getProperty("metadata.broker.list").split(",");
			else if (m_cc.containsKey("bootstrap.servers"))
				brokerList = m_cc.getProperty("bootstrap.servers").split(",");
			for (String broker : brokerList) {
				brokers.add(new Pair<>(broker.split(":")[0], Integer.parseInt(broker.split(":")[1])));
			}

			TopicMetadataResponse topicMetadataRes = SimpleConsumerUtil.getTopicMetadata(brokers.get(0).getKey(), brokers
			      .get(0).getValue(), m_topic);
			TopicMetadata topicMetadata = topicMetadataRes.topicsMetadata().get(0);
			for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
				cluster.addPair(String.valueOf(partitionMetadata.partitionId()), pair);
			}
		}
		return cluster;
	}

	public StoragePair<Resend> createResendPair() {
		KafkaResendStorage resend = new KafkaResendStorage("resend_" + m_topic, m_pc, m_cc);
		ResendPair pair = new ResendPair(resend, null, Long.MAX_VALUE);
		return pair;
	}

}