package com.ctrip.hermes.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.message.MessageValveChain;
import com.ctrip.hermes.spi.MessageValve;

import static org.junit.Assert.assertEquals;

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

	@Test
	public void testAddValve() {
		Producer p = lookup(Producer.class);

		final List<Integer> resultList = new ArrayList<Integer>();

		lookup(MessageRegistry.class).registerValve(new MessageValve() {

			@Override
			public void handle(MessageValveChain chain, MessageContext ctx) {
				resultList.add(10);
				chain.handle(ctx);
			}
		}, "", 10);

		lookup(MessageRegistry.class).registerValve(new MessageValve() {

			@Override
			public void handle(MessageValveChain chain, MessageContext ctx) {
				resultList.add(5);
				chain.handle(ctx);
			}
		}, "", 5);

		p.message("order.new", 12345L).withKey("12345").send();

		assertEquals(Arrays.asList(5, 10), resultList);
	}
}
