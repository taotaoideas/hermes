package com.ctrip.hermes.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandContext;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandRegistry;

public class NettyCommandHandler extends SimpleChannelInboundHandler<Command> {

	@Inject
	private CommandRegistry m_registry;

	private Command m_initCmd;

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Command cmd) throws Exception {
		CommandProcessor processor = m_registry.findProcessor(cmd.getType());

		if (processor == null) {
			System.out.println(String.format("Command processor for type %s is not found", cmd.getType()));
		} else {
			try {
				processor.process(new CommandContext(cmd, ctx));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		if (m_initCmd != null) {
			ctx.channel().writeAndFlush(m_initCmd);
		}

		super.channelActive(ctx);
	}

	public void setInitCmd(Command initCmd) {
		m_initCmd = initCmd;
	}

}
