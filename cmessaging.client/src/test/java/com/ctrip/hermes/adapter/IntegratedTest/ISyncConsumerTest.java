package com.ctrip.hermes.adapter.IntegratedTest;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.cmessaging.client.impl.Config;
import com.ctrip.cmessaging.client.impl.ConsumerFactory;
import com.ctrip.cmessaging.client.impl.ProducerFactory;

import junit.framework.TestCase;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class ISyncConsumerTest {

	// 1. produce() then consumeOne() then ack()
	// 2. produce() then consumeOne() then no ack. restart consume()
	// 3. produce() 2 msgs then consumeOne(), then cosnumeOne()

	final int iterationCount = 1000;
	//	final String exchange = "Flight.Order.StatusNotification";
	final String exchange = "ExchangeTest";
	final String id = "922101_9dc4a4ff";
	String topic = "no.one.use.733";

	@Before
	public void initTopic() {
		consumeOldMessage();
		// todo: wait 10s to server background work (maybe create tables in mysql)
	}

	private void consumeOldMessage() {
		boolean hasMore = true;
		int count = 0;
		while (hasMore) {
			try {
				IMessage msg = syncConsume();
				if (msg == null) {
					hasMore = false;
				} else {
					count++;
				}
			} catch (Exception e) {
				System.out.println("got ConsumeTimeoutException");
				hasMore = false;
			}
		}

		System.out.println(String.format("Consumed %d Old msgs.", count));
	}

	@Test
	public void testIMessageSync() throws IllegalExchangeName, IllegalTopic, ConsumeTimeoutException {
		for (int i = 0; i < iterationCount; i++) {
			produceAndConsume(new Random().nextInt());
		}
	}

	@Test
	public void testIMessageASync() throws IllegalExchangeName, IllegalTopic, ConsumeTimeoutException {
		for (int i = 0; i < iterationCount; i++) {
			produceAndAsyncConsume(new Random().nextInt());
		}
	}

	private void produceAndConsume(int msg) throws ConsumeTimeoutException, IllegalTopic, IllegalExchangeName {
		produce(String.valueOf(msg));
		IMessage receivedMsg1 = syncConsume();


		assertNotNull(receivedMsg1);
		assertEquals(String.valueOf(msg), new String(receivedMsg1.getBody()));
	}

	private void produceAndAsyncConsume(int msg) throws ConsumeTimeoutException, IllegalTopic, IllegalExchangeName {
		produce(String.valueOf(msg));
		IMessage receivedMsg1 = syncConsume();

		assertNotNull(receivedMsg1);
		assertEquals(msg, new String(receivedMsg1.getBody()));
	}

	@Test
	public void testMessageBody() throws ConsumeTimeoutException, IllegalTopic, IllegalExchangeName {
		String msg1 = String.valueOf(new Random().nextInt());
		String msg2 = String.valueOf(new Random().nextInt());
		produce(msg1);
		produce(msg2);
		IMessage receiveMsg1 = syncConsume();

		assertEquals(msg1, new String(receiveMsg1.getBody()));

		IMessage receiveMsg2 = syncConsume();
		TestCase.assertEquals(msg2, new String(receiveMsg2.getBody()));
	}

	@Test
	public void testUserHeader() throws ConsumeTimeoutException, IllegalTopic, IllegalExchangeName {
		String msg1 = String.valueOf(new Random().nextInt());
		String msg2 = String.valueOf(new Random().nextInt());
		produce(msg1);
		syncConsume();
		// assert (Map.toString())produce.header == consumer.getHeader()
	}

	@Test
	public void testDispose() {
	}

	private void produce(String content) {
		Config.setAppId("555555");

		try {
			IProducer producer = ProducerFactory.instance.create(exchange, id);
			producer.PublishAsync(content, topic);
		} catch (IllegalSubject | IllegalExchangeName illegalSubject) {
			illegalSubject.printStackTrace();
		}
	}

	private IMessage syncConsume() throws IllegalTopic, IllegalExchangeName,
			  ConsumeTimeoutException {
		IMessage message1 = null;
		ISyncConsumer consumer1 =
				  ConsumerFactory.instance.createConsumerAsSync
							 (id, topic, exchange, 5000);
		consumer1.setBatchSize(1);

		message1 = consumer1.consumeOne();
		// todo: dispose() here?
		message1.dispose();

		return message1;
	}


	private IMessage AsyncConsume() {
		return null;
	}

}
