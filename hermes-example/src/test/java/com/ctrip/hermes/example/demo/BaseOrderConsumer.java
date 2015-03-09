package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.message.StoredMessage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;

public abstract class BaseOrderConsumer extends BaseConsumer<Order> {

	@Override
	public void consume(StoredMessage<Order> msg) {
		System.out.println(String.format("Consumer %s of %s receive %s", //
		      getId(), getGroupId(), msg.getBody()));

		Cat.logEvent("OrderProcessed", msg.getTopic(), Event.SUCCESS, "key=" + msg.getKey());

		if (shouldNack(msg)) {
			msg.nack();
		}

		if (msg.getBody().getPrice() % 4 == 0) {
			throw new RuntimeException("internal error");
		}
	}

	private boolean shouldNack(StoredMessage<Order> msg) {
		long price = (long) msg.getBody().getPrice();
		boolean nack = price % 3 == System.currentTimeMillis() % 3;

		String prefix = "ACK";
		if (nack) {
			prefix = "NACK";
		}
		System.out.println(String.format("%s %s", prefix, msg.getBody()));

		return nack;
	}

	protected abstract String getGroupId();

	protected abstract String getId();

}
