package com.ctrip.hermes.broker;

import java.util.List;

import com.ctrip.hermes.storage.message.Message;

public interface ProducerChannel {

	public void send(List<Message> msgs);
	
	public void close();

}
