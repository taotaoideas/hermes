package com.ctrip.hermes.message;

public interface MessageManager {

	public MessageValveChain getChain(MessageContext ctx);

}
