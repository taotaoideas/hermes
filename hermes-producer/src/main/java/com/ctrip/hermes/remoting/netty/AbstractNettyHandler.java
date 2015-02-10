package com.ctrip.hermes.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessorManager;

public abstract class AbstractNettyHandler extends SimpleChannelInboundHandler<Command> implements LogEnabled {

	@Inject
	private CommandProcessorManager m_processorManager;

	private List<ChannelEventListener> m_eventListeners = Collections
	      .synchronizedList(new ArrayList<ChannelEventListener>());

	private Channel m_channel;

	private Logger m_logger;

	public void writeCommand(Command cmd) {
		ChannelFuture f = m_channel.writeAndFlush(cmd);

		// TODO
		f.addListener(new GenericFutureListener<Future<? super Void>>() {

			@Override
			public void operationComplete(Future<? super Void> future) throws Exception {
				if (!future.isSuccess()) {
					future.cause().printStackTrace();
				}
			}
		});
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Command cmd) throws Exception {
		m_processorManager.offer(new CommandContext(cmd, this));
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		Channel c = ctx.channel();
		m_logger.error(String.format("Channel exception caught %s", NettyHelper.remoteAddr(c)), cause);
		// TODO
		c.close();
	}

	@Override
	public final void channelActive(ChannelHandlerContext ctx) throws Exception {
		m_channel = ctx.channel();

		doChannelActive(ctx);

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		Channel c = ctx.channel();
		m_logger.info(String.format("Channel inactive %s", NettyHelper.remoteAddr(c)));

		for (ChannelEventListener l : m_eventListeners) {
			try {
				l.onChannelClose(ctx.channel());
			} catch (RuntimeException e) {
				m_logger.error("Listener error when processing channel event ", e);
			}
		}

		super.channelInactive(ctx);
	}

	public void addChannelEventListener(ChannelEventListener listener) {
		m_eventListeners.add(listener);
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	protected void doChannelActive(ChannelHandlerContext ctx) throws Exception {

	}

}
