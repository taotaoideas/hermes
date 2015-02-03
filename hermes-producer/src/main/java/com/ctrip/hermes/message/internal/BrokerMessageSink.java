package com.ctrip.hermes.message.internal;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.netty.RemotingCmd;


public class BrokerMessageSink implements MessageSink {

   public static final String ID = "broker";

   @Inject
   private CodecManager m_codec;

   public BrokerMessageSink() {
	   // TODO Auto-generated constructor stub
   }

	@Override
   public void handle(MessageContext ctx) {
	   // TODO Auto-generated method stub
       Message msg = ctx.getMessage();

       RemotingCmd cmd = new RemotingCmd("send", msg);

       byte[] payload = m_codec.getCodec(msg.getTopic()).encode(cmd);
   }

}
