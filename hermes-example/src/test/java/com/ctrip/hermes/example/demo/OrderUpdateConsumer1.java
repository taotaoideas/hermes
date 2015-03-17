package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.Subscribe;

@Subscribe(topicPattern = "order.update", groupId = "group1")
public class OrderUpdateConsumer1 extends BaseOrderConsumer {

	@Override
	protected String getGroupId() {
		return "group1";
	}

	@Override
	protected String getId() {
		return "order.update.1";
	}

}
