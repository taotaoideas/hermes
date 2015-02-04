package com.ctrip.hermes.netty;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class ProducerNettyClient {

    private static final int PORT = 8007;

    private static final String HOST = "127.0.0.1";
    EventLoopGroup group = new NioEventLoopGroup();

    public void send(RemotingCmd cmd) {

    }

    public void start() {
        try {
            Bootstrap bootstrap = new Bootstrap();

            bootstrap.group(group).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(

//                                    new Encoder(),
//                                    new Decoder(8388608),
//
                                    new MsgToRemotingCmdEncoder(),
//                            new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),

                                    new MsgSenderHandler()
                            );
                        }
                    });
            bootstrap.connect(HOST, PORT).sync().channel().closeFuture();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void shutdown() {
        group.shutdownGracefully();

    }

    public class MsgSenderHandler extends ChannelInboundHandlerAdapter {

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
}
