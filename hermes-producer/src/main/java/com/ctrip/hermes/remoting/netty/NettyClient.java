package com.ctrip.hermes.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;

public class NettyClient extends ContainerHolder {
	@Inject
	private NettyClientConfig m_clientConfig;

	private NettyClientHandler m_cmdHandler;

	public void start(final Command initCmd) {
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			Bootstrap b = new Bootstrap();
			b.group(workerGroup);
			b.channel(NioSocketChannel.class);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			b.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					m_cmdHandler = lookup(NettyClientHandler.class);
					m_cmdHandler.setInitCmd(initCmd);

					ch.pipeline().addLast( //
					      // TODO set max frame length
					      lookup(NettyDecoder.class), //
					      new LengthFieldPrepender(4), //
					      lookup(NettyEncoder.class), //
					      m_cmdHandler);
				}
			});

			b.connect("127.0.0.1", 4376).sync();

			m_cmdHandler.waitUntilActive();

			// Wait until the connection is closed.
			// f.channel().closeFuture().sync();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
	}

	public NettyClientHandler getCmdHandler() {
		return m_cmdHandler;
	}

}
