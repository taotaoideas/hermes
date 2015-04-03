package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.BaseConsumer;
import com.ctrip.hermes.core.message.ConsumerMessage;

public abstract class BaseOrderConsumer extends BaseConsumer<Order> {

	@Override
	public void consume(ConsumerMessage<Order> msg) {
//		System.out.println(String.format("Consumer %s of %s <<<<<<<<<< %s", getId(), getGroupId(), msg.getBody()));
//		System.out.println(((StoredMessage<Order>) msg).getAckOffset());
//
//		Cat.logEvent("OrderProcessed", msg.getTopic(), Event.SUCCESS, "key=" + msg.getKey());
//
//		double price = msg.getBody().getPrice();
//		if (price % 3 == System.currentTimeMillis() % 3) {
//			System.out.println(String.format("\tNACK %s", msg.getKey()));
//			msg.nack();
//		}
//
//		if (price % 4 == 0) {
//			throw new RuntimeException("internal error for demo");
//		}
	}

	protected abstract String getGroupId();

	protected abstract String getId();

}
