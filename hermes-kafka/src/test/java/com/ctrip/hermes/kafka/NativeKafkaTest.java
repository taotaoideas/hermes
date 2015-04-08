package com.ctrip.hermes.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

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

	private static EmbeddedZookeeper zookeeper;

	private static EmbeddedKafka kafka;

	@BeforeClass
	public static void start() {
		zookeeper = new EmbeddedZookeeper();
		kafka = new EmbeddedKafka();
	}

	@AfterClass
	public static void stop() {
		kafka.stop();
		zookeeper.stop();
	}

	@Test
	public void testNative() throws IOException, InterruptedException, ExecutionException {
		String topic = "TOPIC_" + RandomStringUtils.randomAlphabetic(5);

		Properties props = new Properties();
		// Producer
		props.put("metadata.broker.list", EmbeddedKafka.LOCALHOST_BROKER);
		props.put("bootstrap.servers", EmbeddedKafka.LOCALHOST_BROKER);
		props.put("value.serializer", StringSerializer.class.getCanonicalName());
		props.put("key.serializer", StringSerializer.class.getCanonicalName());
		// Consumer
		props.put("zookeeper.connect", EmbeddedZookeeper.ZOOKEEPER_CONNECT);
		props.put("group.id", "GROUP_" + RandomStringUtils.randomAlphabetic(5));

		final List<String> actualResult = new ArrayList<>();
		final List<String> expectedResult = new ArrayList<>();

		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
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
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
		}

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		int i = 1;
		while (i < 10) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, "test-message" + i++);
			Future<RecordMetadata> send = producer.send(data);
			send.get();
			if (send.isDone()) {
				System.out.println("sent: " + data.value());
				expectedResult.add(data.value());
			}
			Thread.sleep(100);
		}

		Assert.assertArrayEquals(expectedResult.toArray(), actualResult.toArray());

		producer.close();
		consumerConnector.shutdown();
	}

}
