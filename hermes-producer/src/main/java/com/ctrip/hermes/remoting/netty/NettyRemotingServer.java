package com.ctrip.hermes.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

public class NettyRemotingServer extends ContainerHolder {
	@Inject
	private NettyServerConfig m_serverConfig;

	public void start() {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		final NettyCommandHandler cmdHandler = lookup(NettyCommandHandler.class);
		final NettyCommandDecoder cmdDecoder = lookup(NettyCommandDecoder.class);
		final NettyCommandEncoder cmdEncoder = lookup(NettyCommandEncoder.class);

		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
			      .childHandler(new ChannelInitializer<SocketChannel>() {
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
			      }).option(ChannelOption.SO_BACKLOG, 128) //
			      .childOption(ChannelOption.SO_KEEPALIVE, true);

			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(m_serverConfig.getListenPort()).sync();

			// Wait until the server socket is closed.
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			// TODO
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

}