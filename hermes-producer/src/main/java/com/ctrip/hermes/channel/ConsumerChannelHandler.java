package com.ctrip.hermes.channel;

import java.util.List;

import com.ctrip.hermes.message.StoredMessage;

public interface ConsumerChannelHandler {

	void handle(List<StoredMessage<byte[]>> msgs);
	
	boolean isOpen();

}
