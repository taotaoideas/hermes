package com.ctrip.hermes.broker;

import java.util.List;

import com.ctrip.hermes.broker.storage.message.Message;

public interface ConsumerChannelHandler {

	void handle(List<Message> msgs);
	
	boolean isOpen();

}
