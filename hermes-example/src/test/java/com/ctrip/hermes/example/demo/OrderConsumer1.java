package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.Subscribe;

@Subscribe(topicPattern = "order.new", groupId = "group1", messageClass = Order.class)
public class OrderConsumer1 extends BaseOrderConsumer {

	@Override
	protected String getGroupId() {
		return "group1";
	}

	@Override
	protected String getId() {
		return "1-A";
	}

}
