package com.ctrip.hermes.message;

public interface MessageSink {

	public void handle(MessageContext ctx);

}
