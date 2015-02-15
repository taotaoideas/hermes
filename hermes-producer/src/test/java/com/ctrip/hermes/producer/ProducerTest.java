package com.ctrip.hermes.producer;

import java.io.IOException;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

public class ProducerTest extends ComponentTestCase {
	@Test
	public void simpleSendWithoutLookup() throws IOException {
		Producer p = Producer.getInstance();

		p.message("order.new", 123456L).send();
	}

	@Test
	public void simpleSend() {
		Producer p = lookup(Producer.class);

		p.message("order.new", "hello").send();
	}

	@Test
	public void sendWithKey() {
		Producer p = lookup(Producer.class);

		p.message("order.new", 12347L).withKey("key12345").send();
	}

	@Test
	public void sendWithPriority() {
		Producer p = lookup(Producer.class);

		p.message("order.new", 12348L).withPriority().send();
	}

}
