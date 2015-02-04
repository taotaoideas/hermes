package com.ctrip.hermes.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;

public class NettyRemotingClient extends ContainerHolder {
	@Inject
	private NettyClientConfig m_clientConfig;

	public void start(Command initCmd) {
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		final NettyCommandDecoder cmdDecoder = lookup(NettyCommandDecoder.class);
		final NettyCommandEncoder cmdEncoder = lookup(NettyCommandEncoder.class);
		final NettyCommandHandler cmdHandler = lookup(NettyCommandHandler.class);
		cmdHandler.setInitCmd(initCmd);

		try {
			Bootstrap b = new Bootstrap();
			b.group(workerGroup);
			b.channel(NioSocketChannel.class);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			b.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast( //
					      // TODO set max frame length
					      new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4), //
					      cmdDecoder, //
					      new LengthFieldPrepender(4), //
					      cmdEncoder, //
					      cmdHandler);
				}
			});

			ChannelFuture f = b.connect("127.0.0.1", 4376).sync();

			// Wait until the connection is closed.
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
		} finally {
			workerGroup.shutdownGracefully();
		}
	}

}
