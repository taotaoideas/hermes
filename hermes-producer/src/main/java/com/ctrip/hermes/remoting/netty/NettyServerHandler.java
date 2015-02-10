package com.ctrip.hermes.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CountDownLatch;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessorManager;

public class NettyServerHandler extends SimpleChannelInboundHandler<Command> implements LogEnabled {

	@Inject
	private CommandProcessorManager m_processorManager;

	private Logger m_logger;

	private Channel m_channel;

	private Command m_initCmd;

	private CountDownLatch m_activeLatch = new CountDownLatch(1);

	public void writeCommand(Command cmd) {
		ChannelFuture f = m_channel.writeAndFlush(cmd);

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
		m_processorManager.offer(new CommandContext(cmd, ctx));
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		m_channel = ctx.channel();

		m_logger.info("New channel connected " + m_channel.remoteAddress());

		if (m_initCmd != null) {
			m_channel.writeAndFlush(m_initCmd);
		}

		m_activeLatch.countDown();

		super.channelActive(ctx);
	}

	public void setInitCmd(Command initCmd) {
		m_initCmd = initCmd;
	}

	public void waitUntilActive() throws InterruptedException {
		m_activeLatch.await();
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
