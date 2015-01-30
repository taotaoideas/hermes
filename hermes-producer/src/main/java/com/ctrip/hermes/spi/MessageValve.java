package com.ctrip.hermes.spi;

import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageValveChain;

public interface MessageValve {

	public void handle(MessageValveChain chain, MessageContext ctx);

}
