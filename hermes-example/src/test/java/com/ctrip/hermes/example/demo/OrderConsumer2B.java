package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.Subscribe;

@Subscribe(topicPattern = "order.new", groupId = "group2", messageClass = Order.class)
public class OrderConsumer2B extends BaseOrderConsumer {

	@Override
	protected String getGroupId() {
		return "group2";
	}

	@Override
	protected String getId() {
		return "2-B";
	}

}
