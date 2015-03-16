package com.ctrip.hermes.example.demo;

import com.ctrip.hermes.consumer.Subscribe;

@Subscribe(topicPattern = "order.new", groupId = "group2")
public class OrderNewConsumer2A extends BaseOrderConsumer {

	@Override
	protected String getGroupId() {
		return "group2";
	}

	@Override
	protected String getId() {
		return "order.new.2.A";
	}

}
