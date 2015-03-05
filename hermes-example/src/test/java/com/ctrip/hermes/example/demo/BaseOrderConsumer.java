package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.message.StoredMessage;

public abstract class BaseOrderConsumer extends BaseConsumer<Order> {

	@Override
	public void consume(StoredMessage<Order> msg) {
		System.out.println(String.format("Consumer %s of %s receive %s", //
		      getId(), getGroupId(), msg.getBody()));

		if (shouldNack(msg)) {
			msg.nack();
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
