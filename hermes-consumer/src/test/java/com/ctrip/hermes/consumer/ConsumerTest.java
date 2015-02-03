package com.ctrip.hermes.consumer;

import org.junit.Test;

import com.ctrip.hermes.message.MessageContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConsumerTest {

	@Test
	public void readAnnotation() {
		Subscribe subAnno = MockConsumer.class.getAnnotation(Subscribe.class);

		assertNotNull(subAnno);
		assertEquals("order.*", subAnno.topicPattern());
		assertEquals("search.order", subAnno.groupId());
	}

	@Subscribe(topicPattern = "order.*", groupId = "search.order")
	static class MockConsumer implements Consumer {

		@Override
		public void consume(MessageContext ctx) {
			// TODO Auto-generated method stub

		}

	}

}
