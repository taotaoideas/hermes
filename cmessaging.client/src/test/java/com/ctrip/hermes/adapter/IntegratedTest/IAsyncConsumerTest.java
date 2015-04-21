package com.ctrip.hermes.adapter.IntegratedTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.cmessaging.client.IAsyncConsumer;
import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.content.AckMode;
import com.ctrip.cmessaging.client.event.IConsumerCallbackEventHandler;
import com.ctrip.cmessaging.client.exception.IllegalExchangeName;
import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.exception.IllegalTopic;
import com.ctrip.cmessaging.client.impl.ConsumerFactory;
import com.ctrip.cmessaging.client.impl.ProducerFactory;

import static org.junit.Assert.assertTrue;

public class IAsyncConsumerTest extends ComponentTestCase {

	@Test
	public void testStopFeature() {

	}

	/**
	 * autoAck设为false，且收到消息不手动ack，则应该会重复发。
	 */
	@Test
	public void testAutoAckFalseFeature() throws IllegalTopic, IllegalExchangeName, IllegalSubject, InterruptedException {
		CountDownLatch latch = new CountDownLatch(4);

		produce();

		IAsyncConsumer consumer1 = ConsumerFactory.instance.createConsumerAsAsync("900205_48db5650", "order_new",
				  "ExchangeTest");
		consumer1.setBatchSize(20);
		consumer1.addConsumerCallbackEventHandler(new MyEventHandler(latch));
		consumer1.ConsumeAsync(1, false);

		assertTrue(latch.await(15, TimeUnit.SECONDS));

	}

	private void produce() throws IllegalExchangeName, IllegalSubject, InterruptedException {
		IProducer producer = ProducerFactory.instance.create("ExchangeTest", "922101_9dc4a4ff");
		producer.PublishAsync("hello i3", "order_new");
		Thread.sleep(1000);

	}


	@Test
	public void testAutoAckTrueFeature() {

	}

	@Test
	public void testMultiThreadFeature() {

	}

	public static class MyEventHandler implements IConsumerCallbackEventHandler {

		CountDownLatch latch;

		public MyEventHandler(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void callback(IMessage message) throws Exception {
			System.out.println("Receive: " + new String(message.getBody()));
			System.out.println(message);

			latch.countDown();
			message.setAcks(AckMode.Nack);
			message.dispose();
		}
	}
}
