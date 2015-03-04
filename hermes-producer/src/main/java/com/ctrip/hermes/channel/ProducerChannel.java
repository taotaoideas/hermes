package com.ctrip.hermes.channel;

import java.util.List;

import com.ctrip.hermes.message.Message;

public interface ProducerChannel {

	public void send(List<Message<byte[]>> msgs);
	
	public void close();

}
