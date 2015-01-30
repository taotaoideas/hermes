package com.ctrip.hermes.producer;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

public class ProducerTest extends ComponentTestCase {
	@Test
	public void simpleWithoutLookup() {
		Producer p = Producer.getInstance();

		p.message("order.new", 12345L).withKey("12345").send();
	}

	@Test
	public void simple() {
		Producer p = lookup(Producer.class);

		p.message("order.new", 12345L).withKey("12345").send();
	}
}
