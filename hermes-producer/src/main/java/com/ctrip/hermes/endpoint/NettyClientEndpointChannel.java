package com.ctrip.hermes.endpoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import com.ctrip.hermes.remoting.codec.NettyDecoder;
import com.ctrip.hermes.remoting.codec.NettyEncoder;
import com.ctrip.hermes.remoting.command.CommandProcessorManager;

public class NettyClientEndpointChannel extends NettyEndpointChannel {

	private String m_host;

	private int m_port;

	public NettyClientEndpointChannel(String host, int port, CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
		m_host = host;
		m_port = port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.channel.EndpointChannel#start()
	 */
	@Override
	public void start() {
		EventLoopGroup workerGroup = new NioEventLoopGroup();

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
					      new NettyDecoder(), //
					      new LengthFieldPrepender(4), //
					      new NettyEncoder(), //
					      this);
				}
			});

			b.connect(m_host, m_port).sync();

			// TODO check connected, otherwise reconnect

		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		} finally {
		}
	}

}
