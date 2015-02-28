package com.ctrip.hermes.storage.storage.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.storage.spi.Storage;
import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public abstract class AbstractKafkaStorage<T> implements Storage<T> {

	private String topic;

	private Producer<String, T> producer;

	private Map<Integer, SimpleConsumer> consumers = new HashMap<>();

	private int bufferSize = 100;

	private List<Pair<String, Integer>> brokers = new ArrayList<>();

	public AbstractKafkaStorage(String id, ProducerConfig pc, ConsumerConfig cc) {
		this.topic = id;
		this.producer = new Producer<>(pc);
		String[] brokerList = cc.props().getString("metadata.broker.list").split(",");
		for (String broker : brokerList) {
			brokers.add(new Pair<>(broker.split(":")[0], Integer.parseInt(broker.split(":")[1])));
		}

		TopicMetadata topicMetadata = SimpleConsumerUtil
		      .getTopicMetadata(brokers.get(0).getKey(), brokers.get(0).getValue(), topic).topicsMetadata().get(0);

		for (PartitionMetadata partition : topicMetadata.partitionsMetadata()) {
			PartitionMetadata metadata = SimpleConsumerUtil.findLeader(brokers, topic, partition.partitionId());
			if (metadata == null) {
				System.out.println("Can't find metadata for Topic and Partition. Exiting");
				return;
			}
			if (metadata.leader() == null) {
				System.out.println("Can't find Leader for Topic and Partition. Exiting");
				return;
			}
			String leadBroker = metadata.leader().host();
			String clientName = "Client_" + topic + "_" + partition.partitionId();

			SimpleConsumer consumer = new SimpleConsumer(leadBroker, metadata.leader().port(), bufferSize, 64 * 1024,
			      clientName);
			consumers.put(partition.partitionId(), consumer);
		}
	}

	@Override
	public void append(List<T> payloads) throws StorageException {
		for (T payload : payloads) {
			KeyedMessage<String, T> kafkaMsg = new KeyedMessage<>(topic, payload);
			producer.send(kafkaMsg);
		}
	}

	@Override
	public Browser<T> createBrowser(long offset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<T> read(Range range) throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T top() throws StorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return this.topic;
	}

}
