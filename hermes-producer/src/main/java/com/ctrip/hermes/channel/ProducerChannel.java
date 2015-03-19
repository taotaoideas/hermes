package com.ctrip.hermes.channel;

import java.util.List;

import com.ctrip.hermes.message.ProducerMessage;

public interface ProducerChannel {

	public List<SendResult> send(List<ProducerMessage<byte[]>> msgs);
	
	public void close();

}
