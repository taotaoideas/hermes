package com.ctrip.hermes.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import kafka.admin.AdminUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class NativeKafkaTest {

	private static MockZookeeper zookeeper;

	private static MockKafka kafka;

	@BeforeClass
	public static void start() {
		zookeeper = new MockZookeeper();
		kafka = new MockKafka();
		MockKafka.LOCALHOST_BROKER = "103.6.237:9092,10.3.6.239:9092,10.3.6.24:9092";
		MockZookeeper.ZOOKEEPER_CONNECT = "10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181";
	}

	@AfterClass
	public static void stop() {
		kafka.stop();
		zookeeper.stop();
	}

	@Test
	public void testNative() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTopic";
		// ZkClient zkClient = new ZkClient(MockZookeeper.ZOOKEEPER_CONNECT);
		// int partition = 1;
		// int replication = 1;
		// AdminUtils.createTopic(zkClient, topic, partition, replication, new Properties());
		int msgNum = 100;
		final CountDownLatch countDown = new CountDownLatch(msgNum);

		Properties produerProps = new Properties();
		// Producer
		produerProps.put("metadata.broker.list", MockKafka.LOCALHOST_BROKER);
		produerProps.put("bootstrap.servers", MockKafka.LOCALHOST_BROKER);
		produerProps.put("value.serializer", StringSerializer.class.getCanonicalName());
		produerProps.put("key.serializer", StringSerializer.class.getCanonicalName());
		// Consumer
		Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", MockZookeeper.ZOOKEEPER_CONNECT);
		consumerProps.put("group.id", "GROUP_" + RandomStringUtils.randomAlphabetic(5));

		final List<String> actualResult = new ArrayList<>();
		final List<String> expectedResult = new ArrayList<>();

		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, 1);
		final List<KafkaStream<String, String>> streams = consumerConnector.createMessageStreams(topicCountMap,
		      new StringDecoder(null), new StringDecoder(null)).get(topic);
		for (final KafkaStream<String, String> stream : streams) {
			new Thread() {
				public void run() {
					for (MessageAndMetadata<String, String> msgAndMetadata : stream) {
						try {
							System.out.println("received: " + msgAndMetadata.message());
							actualResult.add(msgAndMetadata.message());
							countDown.countDown();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
		}

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(produerProps);
		int i = 0;
		while (i < msgNum) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, "test-message" + i++);
			Future<RecordMetadata> send = producer.send(data);
			send.get();
			if (send.isDone()) {
				System.out.println("sending: " + data.value());
				expectedResult.add(data.value());
			}
		}

		countDown.await();

		Assert.assertArrayEquals(expectedResult.toArray(), actualResult.toArray());

		consumerConnector.shutdown();
		producer.close();
	}
}
