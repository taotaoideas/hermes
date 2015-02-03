package com.ctrip.hermes.broker.netty;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NettyClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8007;

    public static void main(String[] args) throws InterruptedException {

        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();

            bootstrap.group(group).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(

                                    new Encoder(),
                                    new Decoder(8388608),
//
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),

                                    new MsgSendHandler(),
                            new LoggingHandler(LogLevel.INFO)
                            );
                        }
                    });
            bootstrap.connect(HOST, PORT).sync().channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
