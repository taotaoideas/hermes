package com.ctrip.hermes.broker.transport;

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
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.codec.NettyDecoder;
import com.ctrip.hermes.core.transport.codec.NettyEncoder;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.NettyServerEndpointChannel;

@Named(type = NettyServer.class)
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
					            new NettyDecoder(), //
					            new LengthFieldPrepender(4), //
					            new NettyEncoder(), //
					            new NettyServerEndpointChannel(lookup(CommandProcessorManager.class)));
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