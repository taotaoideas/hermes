package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.consumer.Message;

public abstract class BaseOrderConsumer extends BaseConsumer<Order> {

	@Override
	public void consume(Message<Order> msg) {
		if (((long) msg.getBody().getPrice()) % 3 == System.currentTimeMillis() % 3) {
			msg.nack();
		}
		System.out.println(String.format("Consumer %s of %s receive %s", getId(), getGroupId(), msg.getBody()));
	}

	protected abstract String getGroupId();

	protected abstract String getId();

}
