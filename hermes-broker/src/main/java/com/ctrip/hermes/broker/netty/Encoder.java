package com.ctrip.hermes.broker.netty;

import java.io.Serializable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public class Encoder extends ObjectEncoder {
}
