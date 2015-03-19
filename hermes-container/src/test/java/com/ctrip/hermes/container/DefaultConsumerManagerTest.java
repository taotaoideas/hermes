package com.ctrip.hermes.container;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.Message;

public class DefaultConsumerManagerTest extends ComponentTestCase {

	public static class TestConsumer implements Consumer<Object> {

		private CountDownLatch m_latch;

		public TestConsumer(CountDownLatch latch) {
			m_latch = latch;
		}

		@Override
		public void consume(List<Message<Object>> msgs) {
			System.out.println("Receive message " + msgs);
			m_latch.countDown();
		}

	}

	@Test
	public void test() throws Exception {
		ConsumerBootstrap m = lookup(ConsumerBootstrap.class);

		CountDownLatch latch = new CountDownLatch(1);
		Subscriber s = new Subscriber("order.new", "groupId", new TestConsumer(latch));
		m.startConsumer(s);

		latch.await(2, TimeUnit.SECONDS);
	}

}
