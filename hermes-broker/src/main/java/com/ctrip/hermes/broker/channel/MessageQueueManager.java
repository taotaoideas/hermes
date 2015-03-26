package com.ctrip.hermes.broker.channel;

import com.ctrip.hermes.remoting.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.storage.MessageQueue;

public interface MessageQueueManager {

	public MessageQueue findQueue(Tpp tpp);
	

}
