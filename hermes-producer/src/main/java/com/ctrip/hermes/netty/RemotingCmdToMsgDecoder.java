package com.ctrip.hermes.netty;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class RemotingCmdToMsgDecoder extends MessageToMessageDecoder<Object>{
    @Override
    protected void decode(ChannelHandlerContext ctx, Object msg, List out) throws Exception {

    }
}
