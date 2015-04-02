package com.ctrip.hermes;

import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class EmbeddedKafka {

	public static final String LOG_DIR = System.getProperty("java.io.tmpdir") + RandomStringUtils.randomAlphabetic(5);

	public static final String BROKER_ID = "0";

	public static final String BROKER_HOSTNAME = "localhost";

	public static final String BROKER_PORT = "9092";

	public static final String LOCALHOST_BROKER = BROKER_HOSTNAME + ":" + BROKER_PORT;

	public KafkaServerStartable kafkaServer;

	public EmbeddedKafka() {
		this(LOG_DIR, BROKER_PORT, BROKER_ID);
		start();
	}

	private EmbeddedKafka(Properties properties) {
		KafkaConfig kafkaConfig = new KafkaConfig(properties);
		kafkaServer = new KafkaServerStartable(kafkaConfig);
	}

	private EmbeddedKafka(String logDir, String port, String brokerId) {
		this(createProperties(logDir, port, brokerId));
	}

	private static Properties createProperties(String logDir, String port, String brokerId) {
		Properties properties = new Properties();
		properties.put("port", port);
		properties.put("broker.id", brokerId);
		properties.put("log.dirs", LOG_DIR);
		properties.put("zookeeper.connect", EmbeddedZookeeper.ZOOKEEPER_CONNECT);
		return properties;
	}

	public void start() {
		kafkaServer.startup();
		System.out.println("embedded kafka is up");
	}

	public void stop() {
		kafkaServer.shutdown();
		System.out.println("embedded kafka stop");
	}

}