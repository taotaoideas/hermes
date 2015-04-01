package com.ctrip.hermes.container;

import org.unidal.lookup.ComponentTestCase;

public class DefaultConsumerManagerTest extends ComponentTestCase {

//	public static class TestConsumer implements Consumer<Object> {
//
//		private CountDownLatch m_latch;
//
//		public TestConsumer(CountDownLatch latch) {
//			m_latch = latch;
//		}
//
//		@Override
//		public void consume(List<ConsumerMessage<Object>> msgs) {
//			System.out.println("Receive message " + msgs);
//			m_latch.countDown();
//		}
//
//	}
//
//	@Test
//	public void test() throws Exception {
//		ConsumerBootstrap m = lookup(ConsumerBootstrap.class);
//
//		CountDownLatch latch = new CountDownLatch(1);
//		Subscriber s = new Subscriber("order.new", "groupId", new TestConsumer(latch));
//		m.startConsumer(s);
//
//		latch.await(2, TimeUnit.SECONDS);
//	}

}
