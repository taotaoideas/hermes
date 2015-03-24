package com.ctrip.hermes.channel;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ctrip.hermes.remoting.command.Ack;
import com.ctrip.hermes.remoting.command.AckAware;
import com.ctrip.hermes.remoting.command.Command;
import com.ctrip.hermes.remoting.netty.NettyDecoder;
import com.ctrip.hermes.remoting.netty.NettyEncoder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class RemoteEndpointChannel extends SimpleChannelInboundHandler<Command> implements EndpointChannel {
	private ConcurrentMap<Long, AckAware<Ack>> m_pendingCommands = new ConcurrentHashMap<>();

	private String m_host;

	private int m_port;

	private Channel m_channel;

	/**
	 * @param connectionString
	 */
	public RemoteEndpointChannel(String host, int port) {
		m_host = host;
		m_port = port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.channel.EndpointChannel#write(com.ctrip.hermes.remoting.command.Command)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void write(Command command) {
		if (command instanceof AckAware) {
			m_pendingCommands.put(command.getHeader().getCorrelationId(), (AckAware<Ack>) command);
		}

		m_channel.writeAndFlush(command);
	}

	// @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Command command) throws Exception {
		if (command instanceof Ack) {
			long correlationId = command.getHeader().getCorrelationId();
			AckAware<Ack> reqCommand = m_pendingCommands.get(correlationId);
			if (reqCommand != null) {
				reqCommand.onAck((Ack) command);
				m_pendingCommands.remove(correlationId);
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#channelActive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		m_channel = ctx.channel();
		super.channelActive(ctx);
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
