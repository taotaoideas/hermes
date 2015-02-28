package com.ctrip.hermes.storage.storage.kafka;

import java.util.Arrays;

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.ClusteredMessagePair;
import com.ctrip.hermes.storage.pair.MessagePair;
import com.ctrip.hermes.storage.pair.ResendPair;
import com.ctrip.hermes.storage.pair.StoragePair;

public class KafkaGroup {

	private String m_topic;

	private String m_groupId;

	private ProducerConfig m_pc;

	private ConsumerConfig m_cc;

	public KafkaGroup(String topic, String groupId, ProducerConfig pc, ConsumerConfig cc) {
		this.m_topic = topic;
		this.m_groupId = groupId;
		this.m_pc = pc;
		this.m_cc = cc;
	}

	public StoragePair<Message> createMessagePair() {
		KafkaMessageStorage main = new KafkaMessageStorage(m_topic, m_pc, m_cc);
		KafkaOffsetStorage offset = new KafkaOffsetStorage("offset_" + m_topic + "_" + m_groupId, m_pc, m_cc);

		MessagePair pair = new MessagePair(main, offset);

		return new ClusteredMessagePair(Arrays.asList(pair));

	}

	public StoragePair<Resend> createResendPair() {
		KafkaResendStorage resend = new KafkaResendStorage("resend_" + m_topic + "_" + m_groupId, m_pc, m_cc);
		KafkaOffsetStorage offset = new KafkaOffsetStorage("offset_resend_" + m_topic + "_" + m_groupId, m_pc, m_cc);
		ResendPair pair = new ResendPair(resend, offset, Long.MAX_VALUE);
		return pair;
	}

}
