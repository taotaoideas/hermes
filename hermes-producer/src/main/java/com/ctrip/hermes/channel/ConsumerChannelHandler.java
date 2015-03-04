package com.ctrip.hermes.channel;

import java.util.List;

import com.ctrip.hermes.storage.message.Record;

public interface ConsumerChannelHandler {

	void handle(List<Record> msgs);
	
	boolean isOpen();

}
