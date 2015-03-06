package com.ctrip.hermes.channel;

import java.util.List;

import com.ctrip.hermes.message.Message;

public interface ProducerChannel {

	public List<SendResult> send(List<Message<byte[]>> msgs);
	
	public void close();

}
