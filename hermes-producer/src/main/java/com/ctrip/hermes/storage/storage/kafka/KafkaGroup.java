package com.ctrip.hermes.storage.storage.kafka;

import java.util.Arrays;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.storage.pair.MessagePair;
import com.ctrip.hermes.storage.pair.ResendPair;
import com.ctrip.hermes.storage.pair.StoragePair;

public class KafkaGroup {

	private String m_topic;

	private String m_groupId;

	private String m_partition;

	private ProducerConfig m_pc;

	private ConsumerConfig m_cc;

	public KafkaGroup(String topic, String groupId, String partition, ProducerConfig pc, ConsumerConfig cc) {
		m_topic = topic;
		m_groupId = groupId;
		m_partition = partition;
		m_pc = pc;
		m_cc = cc;
	}

	public StoragePair<Record> createMessagePair() {
		KafkaMessageStorage main = new KafkaMessageStorage(m_topic, m_pc, m_cc);
		KafkaOffsetStorage offset = new KafkaOffsetStorage("offset_" + m_topic);

		MessagePair pair = new MessagePair(main, offset);

		return new ClusteredMessagePair(Arrays.asList(pair));

	}

	public StoragePair<Resend> createResendPair() {
		KafkaResendStorage resend = new KafkaResendStorage("resend_" + m_topic + "_" + m_groupId, m_partition, m_pc, m_cc);
		KafkaOffsetStorage offset = new KafkaOffsetStorage("offset_resend_" + m_topic);
		ResendPair pair = new ResendPair(resend, offset, Long.MAX_VALUE);
		return pair;
	}

}