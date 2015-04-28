package com.ctrip.hermes.kafka.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class HermesConsumerPerf {

	private static class ConsumerPerf extends BaseConsumer<byte[]> {

		private Integer threadId;

		private String name;

		private ConsumerPerfConfig config;

		private AtomicLong totalMessagesRead;

		private AtomicLong totalBytesRead;

		private long bytesRead = 0L;

		private long messagesRead = 0L;

		private long startMs = System.currentTimeMillis();

		private long lastReportTime = startMs;

		private long lastBytesRead = 0L;

		private long lastMessagesRead = 0L;

		public ConsumerPerf(Integer threadId, String name, ConsumerPerfConfig config, AtomicLong totalMessagesRead,
		      AtomicLong totalBytesRead) {
			this.threadId = threadId;
			this.name = name;
			this.config = config;
			this.totalMessagesRead = totalMessagesRead;
			this.totalBytesRead = totalBytesRead;
		}

		@Override
		protected void consume(ConsumerMessage<byte[]> msg) {
			byte[] event = msg.getBody();
			messagesRead += 1;
			bytesRead += event.length;
			System.out.println(messagesRead);
			if (messagesRead % config.reportingInterval == 0) {
				printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime,
				      System.currentTimeMillis());
				lastReportTime = System.currentTimeMillis();
				lastMessagesRead = messagesRead;
				lastBytesRead = bytesRead;
			}
		}

		private void printMessage(Integer id, Long bytesRead, Long lastBytesRead, Long messagesRead,
		      Long lastMessagesRead, Long startMs, Long endMs) {
			long elapsedMs = endMs - startMs;
			double totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
			double mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
			System.out.println(String.format("%s, %d, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(endMs), id,
			      config.consumerConfig.fetchMessageMaxBytes(), totalMBRead, 1000.0 * (mbRead / elapsedMs), messagesRead,
			      ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0));
		}
	}

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

		Engine engine = Engine.getInstance();

		String topic = config.topic;
		String group = config.consumerConfig.groupId();
		int numThreads = config.numThreads;
		List<Subscriber> subscribers = new ArrayList<>();

		for (int i = 0; i < numThreads; i++) {
			Subscriber s = new Subscriber(topic, group, new ConsumerPerf(i, "kafka-zk-consumer-" + i, config,
			      totalMessagesRead, totalBytesRead));
			subscribers.add(s);
		}

		logger.info("Sleeping for 1 second.");
		Thread.sleep(1000);
		logger.info("starting threads");
		long startMs = System.currentTimeMillis();
		System.out.println("Starting consumer...");
		engine.start(subscribers);

		Thread.currentThread().join();
		long endMs = System.currentTimeMillis();
		double elapsedSecs = (endMs - startMs - config.consumerConfig.consumerTimeoutMs()) / 1000.0;
		if (!config.showDetailedStats) {
			double totalMBRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
			System.out.println(String.format("%s, %s, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(startMs),
			      config.dateFormat.format(endMs), config.consumerConfig.fetchMessageMaxBytes(), totalMBRead, totalMBRead
			            / elapsedSecs, totalMessagesRead.get(), totalMessagesRead.get() / elapsedSecs));
		}
		System.exit(0);
	}

	private static Logger logger = Logger.getLogger(HermesConsumerPerf.class);
}