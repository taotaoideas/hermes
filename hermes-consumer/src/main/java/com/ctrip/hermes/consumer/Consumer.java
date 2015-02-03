package com.ctrip.hermes.consumer;

import com.ctrip.hermes.message.MessageContext;

public interface Consumer {

	public void consume(MessageContext ctx);
	
}
