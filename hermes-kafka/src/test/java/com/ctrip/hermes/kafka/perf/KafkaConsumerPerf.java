package com.ctrip.hermes.kafka.perf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.ZkUtils;

import org.apache.log4j.Logger;

public class KafkaConsumerPerf {

	private static class ConsumerPerfThread extends Thread {
		private Integer threadId;

		private String name;

		private KafkaStream<byte[], byte[]> stream;

		private ConsumerPerfConfig config;

		private AtomicLong totalMessagesRead;

		private AtomicLong totalBytesRead;

		public ConsumerPerfThread(Integer threadId, String name, KafkaStream<byte[], byte[]> stream,
		      ConsumerPerfConfig config, AtomicLong totalMessagesRead, AtomicLong totalBytesRead) {
			this.threadId = threadId;
			this.name = name;
			this.stream = stream;
			this.config = config;
			this.totalMessagesRead = totalMessagesRead;
			this.totalBytesRead = totalBytesRead;
		}

		private void printMessage(Integer id, Long bytesRead, Long lastBytesRead, Long messagesRead,
		      Long lastMessagesRead, Long startMs, Long endMs) {
			long elapsedMs = endMs - startMs;
			double totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
			double mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
			System.out.println(("%s, %d, %d, %.4f, %.4f, %d, %.4f").format(config.dateFormat.format(endMs), id,
			      config.consumerConfig.fetchMessageMaxBytes(), totalMBRead, 1000.0 * (mbRead / elapsedMs), messagesRead,
			      ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0));
		}

		public void run() {
			long bytesRead = 0L;
			long messagesRead = 0L;
			long startMs = System.currentTimeMillis();
			long lastReportTime = startMs;
			long lastBytesRead = 0L;
			long lastMessagesRead = 0L;

			try {
				ConsumerIterator<byte[], byte[]> iter = stream.iterator();
				while (iter.hasNext() && messagesRead < config.numMessages) {
					MessageAndMetadata<byte[], byte[]> messageAndMetadata = iter.next();
					messagesRead += 1;
					bytesRead += messageAndMetadata.message().length;
					if (messagesRead % config.reportingInterval == 0) {
						if (config.showDetailedStats)
							printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime,
							      System.currentTimeMillis());
						lastReportTime = System.currentTimeMillis();
						lastMessagesRead = messagesRead;
						lastBytesRead = bytesRead;
					}
				}
			} catch (ConsumerTimeoutException e) {
				System.err.println(name + " ConsumerTimeOut");
			} catch (Exception e) {
				e.printStackTrace();
			}
			totalMessagesRead.addAndGet(messagesRead);
			totalBytesRead.addAndGet(bytesRead);
			if (config.showDetailedStats)
				printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs,
				      System.currentTimeMillis());
		}
	}

	private static Logger logger = Logger.getLogger(KafkaConsumerPerf.class);

	public static void main(String[] args) throws InterruptedException {
		ConsumerPerfConfig config = new ConsumerPerfConfig(args);
		logger.info("Starting consumer...");
		AtomicLong totalMessagesRead = new AtomicLong(0);
		AtomicLong totalBytesRead = new AtomicLong(0);
		if (!config.hideHeader) {
			if (!config.showDetailedStats)
				System.out
				      .println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
			else
				System.out.println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
		}

		// clean up zookeeper state for this group id for every perf run
		ZkUtils.maybeDeletePath(config.consumerConfig.zkConnect(), "/consumers/" + config.consumerConfig.groupId());

		ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(config.consumerConfig);

		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put(config.topic, config.numThreads);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector
		      .createMessageStreams(topicMap);
		List<ConsumerPerfThread> threadList = new ArrayList<>();
		for (Entry<String, List<KafkaStream<byte[], byte[]>>> entry : topicMessageStreams.entrySet()) {
			for (int i = 0; i < entry.getValue().size(); i++) {
				ConsumerPerfThread thread = new ConsumerPerfThread(i, "kafka-zk-consumer-" + i, entry.getValue().get(i),
				      config, totalMessagesRead, totalBytesRead);
				threadList.add(thread);
			}
		}

		logger.info("Sleeping for 1 second.");
		Thread.sleep(1000);
		logger.info("starting threads");
		long startMs = System.currentTimeMillis();
		for (Thread thread : threadList)
			thread.start();

		for (Thread thread : threadList)
			thread.join();

		long endMs = System.currentTimeMillis();
		double elapsedSecs = (endMs - startMs - config.consumerConfig.consumerTimeoutMs()) / 1000.0;
		if (!config.showDetailedStats) {
			double totalMBRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
			System.out.println(("%s, %s, %d, %.4f, %.4f, %d, %.4f").format(config.dateFormat.format(startMs),
			      config.dateFormat.format(endMs), config.consumerConfig.fetchMessageMaxBytes(), totalMBRead, totalMBRead
			            / elapsedSecs, totalMessagesRead.get(), totalMessagesRead.get() / elapsedSecs));
		}
		System.exit(0);
	}
}
