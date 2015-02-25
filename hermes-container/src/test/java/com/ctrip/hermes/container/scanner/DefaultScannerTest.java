package com.ctrip.hermes.container.scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.ctrip.hermes.consumer.BackoffException;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.consumer.Subscribe;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.engine.scanner.DefaultScanner;

public class DefaultScannerTest {

	@Subscribe(topicPattern = "order.new", groupId = "group1")
	public static class MockConsumer implements Consumer<Object> {

		@Override
		public void consume(List<Message<Object>> msgs) throws BackoffException {
		}

	}

	@Test
	public void test() {
		List<Subscriber> ss = new DefaultScanner().scan();

		assertNotNull(ss);

		boolean found = false;
		for (Subscriber s : ss) {
			if (s.getConsumer().getClass().equals(MockConsumer.class)) {
				found = true;
				assertEquals("order.new", s.getTopicPattern());
				assertEquals("group1", s.getGroupId());
			}
		}

		assertTrue(found);
	}

}
