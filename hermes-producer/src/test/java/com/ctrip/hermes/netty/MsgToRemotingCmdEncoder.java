package com.ctrip.hermes.netty;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.message.Message;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class MsgToRemotingCmdEncoder extends MessageToMessageEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        RemotingCmd cmd = new RemotingCmd();
        cmd.setType(RemotingCmd.REQUEST_TYPE);
        cmd.setBody(JSON.toJSONBytes(msg));

        out.add(cmd);
    }
}
