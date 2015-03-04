package com.ctrip.hermes.storage.storage.kafka;

import java.util.ArrayList;
import java.util.List;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.spi.Storage;

public abstract class AbstractKafkaStorage<T> implements Storage<T> {

	protected String m_topic;

	protected Producer<String, byte[]> m_producer;

	protected String m_partition;

	protected int m_partition_id;

	protected SimpleConsumer m_consumer;

	protected int bufferSize = 10000;

	protected List<Pair<String, Integer>> brokers = new ArrayList<>();

	public AbstractKafkaStorage(String id, String partition, ProducerConfig pc, ConsumerConfig cc) {
		m_topic = id;
		m_partition = partition;
		m_producer = new Producer<>(pc);
		String[] brokerList = cc.props().getString("metadata.broker.list").split(",");
		for (String broker : brokerList) {
			brokers.add(new Pair<>(broker.split(":")[0], Integer.parseInt(broker.split(":")[1])));
		}

		TopicMetadata topicMetadata = SimpleConsumerUtil
		      .getTopicMetadata(brokers.get(0).getKey(), brokers.get(0).getValue(), m_topic).topicsMetadata().get(0);
		if (topicMetadata.topic() == null) {
			System.out.println("Can't find topic. Exiting");
			return;
		}
		if (topicMetadata.partitionsMetadata().size() == 0) {
			System.out.println("Can't find partition for topic. Exiting");
			return ;
		}

		m_partition_id = partition.hashCode() % topicMetadata.partitionsMetadata().size();
		PartitionMetadata metadata = SimpleConsumerUtil.findLeader(brokers, m_topic, m_partition_id);
		if (metadata == null) {
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + m_topic + "_" + m_partition_id;

		m_consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), bufferSize, 64 * 1024, clientName);
	}

	@Override
	public String getId() {
		return this.m_topic;
	}
}
