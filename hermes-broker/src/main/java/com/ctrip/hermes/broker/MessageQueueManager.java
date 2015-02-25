package com.ctrip.hermes.broker;

import com.ctrip.hermes.storage.MessageQueue;

public interface MessageQueueManager {

	public MessageQueue findQueue(String topic, String groupId);

	public MessageQueue findQueue(String topic);

}
