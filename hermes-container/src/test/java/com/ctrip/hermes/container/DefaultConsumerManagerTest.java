package com.ctrip.hermes.container;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.message.PipelineContext;

public class DefaultConsumerManagerTest extends ComponentTestCase {

	public static class TestConsumer implements Consumer<Object> {

		private CountDownLatch m_latch;

		public TestConsumer(CountDownLatch latch) {
			m_latch = latch;
		}

		@Override
		public void consume(PipelineContext<Object> ctx) {
			System.out.println("Receive message " + ctx.getMessage());
			m_latch.countDown();
		}

	}

	@Test
	public void test() throws InterruptedException {
		ConsumerManager m = lookup(ConsumerManager.class);

		CountDownLatch latch = new CountDownLatch(1);
		Subscriber s = new Subscriber("groupId", "order.new", new TestConsumer(latch));
		m.startConsumer(s);

		latch.await(2, TimeUnit.SECONDS);
	}

}
