package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

	private AtomicReference<Channel> m_channel = new AtomicReference<>(null);

	private AtomicBoolean m_writerStarted = new AtomicBoolean(false);

	private Thread m_writer;

	protected CommandProcessorManager m_cmdProcessorManager;

	protected List<EndpointChannelEventListener> m_listeners = new CopyOnWriteArrayList<>();

	protected AtomicBoolean m_closed = new AtomicBoolean(false);

	// TODO config size
	private BlockingQueue<Command> m_writeQueue = new LinkedBlockingQueue<Command>();

	public NettyEndpointChannel(CommandProcessorManager cmdProcessorManager) {
		m_cmdProcessorManager = cmdProcessorManager;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void writeCommand(Command command) {
		if (m_writerStarted.compareAndSet(false, true)) {
			startWriter();
		}

		if (command instanceof AckAware) {
			m_pendingCommands.put(command.getHeader().getCorrelationId(), (AckAware<Ack>) command);
		}

		// TODO if full?
		m_writeQueue.offer(command);
	}

	private void startWriter() {
		m_writer = new Thread() {
			@Override
			public void run() {
				Command cmd = null;
				while (!Thread.currentThread().isInterrupted()) {
					try {

						if (cmd == null) {
							cmd = m_writeQueue.take();
						}

						Channel channel = m_channel.get();

						if (channel != null && channel.isWritable()) {
							ChannelFuture future = channel.writeAndFlush(cmd).sync();
							if (future.isSuccess()) {
								cmd = null;
							}
						}

					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						// TODO
					}
				}
			}
		};

		// TODO
		m_writer.setDaemon(true);
		m_writer.setName("NettyWriter");
		m_writer.start();
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
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Channel inactive...");
		m_channel.set(null);
		notifyListener(new EndpointChannelInactiveEvent(ctx, this));
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Channel active...");
		m_channel.set(ctx.channel());
		notifyListener(new EndpointChannelActiveEvent(ctx, this));
		super.channelActive(ctx);

	}

	@Override
	public void addListener(EndpointChannelEventListener... listeners) {
		if (listeners != null) {
			m_listeners.addAll(Arrays.asList(listeners));
		}
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

	@Override
	public boolean isClosed() {
		return m_channel.get() == null;
	}

}
