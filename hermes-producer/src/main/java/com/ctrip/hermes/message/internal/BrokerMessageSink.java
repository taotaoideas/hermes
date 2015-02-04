package com.ctrip.hermes.message.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.netty.ProducerNettyClient;
import com.ctrip.hermes.netty.RemotingCmd;
import com.ctrip.hermes.netty.RemotingCmdHelper;


public class BrokerMessageSink implements Initializable, MessageSink {

    public static final String ID = "broker";

    ProducerNettyClient nettyClient;


    @Inject
    private CodecManager m_codec;

    public BrokerMessageSink() {
        // init ProducerNettyClient

        // TODO Auto-generated constructor stub
    }

    @Override
    public void handle(MessageContext ctx) {
        Message msg = ctx.getMessage();

        byte[] body = m_codec.getCodec(msg.getTopic()).encode(msg);
        RemotingCmd cmd = RemotingCmdHelper.buildRequestCmd(body);

        nettyClient.send(cmd);
        nettyClient.shutdown();
    }

    @Override
    public void initialize() throws InitializationException {
        nettyClient = new ProducerNettyClient();
        nettyClient.start();
    }
}
