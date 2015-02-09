package com.ctrip.hermes.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;

public class ConsumerTest {

	@Test
	public void readAnnotation() {
		Subscribe subAnno = MockConsumer.class.getAnnotation(Subscribe.class);

		assertNotNull(subAnno);
		assertEquals("order.*", subAnno.topicPattern());
		assertEquals("search.order", subAnno.groupId());
	}

	@Subscribe(topicPattern = "order.*", groupId = "search.order")
	public static class MockConsumer implements Consumer<Object> {

		@Override
		public void consume(List<Object> msgs) {
		}

	}

}
