package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.Subscribe;

@Subscribe(topicPattern = "order_new", groupId = "group1")
public class OrderNewConsumer1 extends BaseOrderConsumer {

	@Override
	protected String getGroupId() {
		return "group1";
	}

	@Override
	protected String getId() {
		return "order.new.1";
	}

}
