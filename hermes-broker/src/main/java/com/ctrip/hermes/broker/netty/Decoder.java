package com.ctrip.hermes.broker.netty;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.serialization.ClassResolver;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;

public class Decoder extends ObjectDecoder {
    public Decoder(int maxObjectSize) {
        super(maxObjectSize, ClassResolvers.cacheDisabled(null));
    }
}
