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

		return price % 3 == System.currentTimeMillis() % 3;
	}

	protected abstract String getGroupId();

	protected abstract String getId();

}
