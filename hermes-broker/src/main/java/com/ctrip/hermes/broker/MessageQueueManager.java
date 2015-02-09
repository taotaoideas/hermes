package com.ctrip.hermes.broker;

import com.ctrip.hermes.broker.storage.impl.StorageMessageQueue;

public interface MessageQueueManager {

	public StorageMessageQueue findQueue(String topic, String groupId);

	public StorageMessageQueue findQueue(String topic);

}
