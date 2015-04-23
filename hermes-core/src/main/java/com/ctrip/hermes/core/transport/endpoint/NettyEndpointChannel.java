package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctrip.hermes.core.transport.command.Ack;
import com.ctrip.hermes.core.transport.command.AckAware;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelActiveEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class NettyEndpointChannel extends SimpleChannelInboundHandler<Command> implements EndpointChannel {
	private ConcurrentMap<Long, AckAware<Ack>> m_pendingCommands = new ConcurrentHashMap<>();

	private Channel m_channel;

	protected CommandProcessorManager m_cmdProcessorManager;

	protected List<EndpointChannelEventListener> m_listeners = new CopyOnWriteArrayList<>();

	protected AtomicBoolean m_active = new AtomicBoolean(false);

	protected AtomicBoolean m_writable = new AtomicBoolean(true);

	// TODO config size
	private BlockingQueue<Command> m_delayWQueue = new LinkedBlockingQueue<Command>();

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
	public void writeCommand(Command command) {
		if (command instanceof AckAware) {
			m_pendingCommands.put(command.getHeader().getCorrelationId(), (AckAware<Ack>) command);
		}

		if (isWritable()) {
			// TODO
			System.out.println("Write through...");
			m_channel.writeAndFlush(command);
		} else {
			// TODO if full?
			System.out.println("Delay write...");
			m_delayWQueue.offer(command);
		}
	}

	private boolean isWritable() {
		return m_channel != null && m_active.get() && m_channel.isWritable();
	}

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
			m_cmdProcessorManager.offer(new CommandProcessorContext(command, this));
		}

	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Writablity changed..." + ctx.channel().isWritable());
		super.channelWritabilityChanged(ctx);
		m_writable.set(ctx.channel().isWritable());
		purgeDelayWQueue();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Channel inactive...");
		m_active.set(false);
		notifyListener(new EndpointChannelInactiveEvent(ctx));
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Channel active...");
		m_channel = ctx.channel();
		super.channelActive(ctx);
		m_active.set(true);
		purgeDelayWQueue();
		notifyListener(new EndpointChannelActiveEvent(ctx));

	}

	protected void purgeDelayWQueue() {
		if (isWritable()) {
			while (!m_delayWQueue.isEmpty()) {
				Command cmd = m_delayWQueue.poll();
				if (cmd != null) {
					m_channel.writeAndFlush(cmd);
				}
			}
		}
	}

	@Override
	public void addListener(EndpointChannelEventListener listener) {
		m_listeners.add(listener);
	}

	protected void notifyListener(EndpointChannelEvent event) {
		for (EndpointChannelEventListener listener : m_listeners) {
			try {
				listener.onEvent(event);
			} catch (Exception e) {
				// TODO
			}
		}
	}
}
