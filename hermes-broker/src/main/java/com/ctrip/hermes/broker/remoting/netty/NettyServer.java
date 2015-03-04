package com.ctrip.hermes.broker.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.netty.NettyDecoder;
import com.ctrip.hermes.remoting.netty.NettyEncoder;

public class NettyServer extends ContainerHolder {
	@Inject
	private NettyServerConfig m_serverConfig;

	public void start() {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
			      .childHandler(new ChannelInitializer<SocketChannel>() {
				      @Override
				      public void initChannel(SocketChannel ch) throws Exception {
					      ch.pipeline().addLast( //
					            // TODO set max frame length
					            lookup(NettyDecoder.class), //
					            new LengthFieldPrepender(4), //
					            lookup(NettyEncoder.class), //
					            lookup(NettyServerHandler.class));
				      }
			      }).option(ChannelOption.SO_BACKLOG, 128) // TODO set tcp options
			      .childOption(ChannelOption.SO_KEEPALIVE, true);

			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(m_serverConfig.getListenPort()).sync();

			// Wait until the server socket is closed.
			 f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			// TODO
			e.printStackTrace();
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		} finally {
		}
	}

}