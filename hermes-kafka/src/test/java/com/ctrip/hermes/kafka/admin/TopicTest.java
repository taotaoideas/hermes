package com.ctrip.hermes.kafka.admin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

public class TopicTest {

	@Test
	public void createTopicInTestEnv() {
		String LOCALHOST_BROKER = "103.6.237:9092,10.3.6.239:9092,10.3.6.24:9092";
		String ZOOKEEPER_CONNECT = "10.3.6.90:2181,10.3.8.62:2181,10.3.8.63:2181";
		ZkClient zkClient = new ZkClient(ZOOKEEPER_CONNECT);
		zkClient.setZkSerializer(new ZKStringSerializer());
		int partition = 1;
		int replication = 1;
		String topic = String.format("kafka.test_create_topic_p%s_r%s", partition, replication);
		if (AdminUtils.topicExists(zkClient, topic)) {
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient);
			System.out.println(topicMetadata);
			AdminUtils.deleteTopic(zkClient, topic);
		}
		AdminUtils.createTopic(zkClient, topic, partition, replication, new Properties());
	}

	@Test
	public void testSerializable() throws IOException {
		// Object serializable = new String("{\"version\":1,\"partitions\":{\"0\":[1]}}");
		Object serializable = new String("1");
		ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
		stream.writeObject(serializable);
		stream.close();
		byte[] result = byteArrayOS.toByteArray();
		System.out.println(Arrays.toString(result));
	}
}
