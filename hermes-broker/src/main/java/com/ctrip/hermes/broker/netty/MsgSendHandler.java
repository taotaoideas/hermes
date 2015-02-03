package com.ctrip.hermes.broker.netty;

import java.nio.channels.Channel;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class MsgSendHandler extends ChannelHandlerAdapter {

    String PING = "PING";

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(PING);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        System.out.println(msg.toString());
//        ctx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
