package com.ctrip.hermes.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ctrip.hermes.remoting.command.Ack;
import com.ctrip.hermes.remoting.command.AckAware;
import com.ctrip.hermes.remoting.command.Command;
import com.ctrip.hermes.remoting.command.CommandContext;
import com.ctrip.hermes.remoting.command.CommandProcessorManager;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class NettyEndpointChannel extends SimpleChannelInboundHandler<Command> implements EndpointChannel {
	private ConcurrentMap<Long, AckAware<Ack>> m_pendingCommands = new ConcurrentHashMap<>();

	private Channel m_channel;
	
	protected CommandProcessorManager m_cmdProcessorManager;

	public NettyEndpointChannel(CommandProcessorManager cmdProcessorManager) {
	   m_cmdProcessorManager = cmdProcessorManager;
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
		} else {
			m_cmdProcessorManager.offer(new CommandContext(command, this));
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

}
