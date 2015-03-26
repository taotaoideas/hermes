package com.ctrip.hermes.remoting.command;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultCommandParser implements CommandParser {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.remoting.command.CommandParser#parse(io.netty.buffer.ByteBuf)
	 */
	@Override
	public Command parse(ByteBuf frame) {
		Header header = new Header();
		header.parse(frame);

		Class<? extends Command> cmdClazz = header.getType().getClazz();
		Command cmd = null;
		try {
			cmd = cmdClazz.newInstance();
			cmd.parse(frame, header);
			return cmd;
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Can not construct command instance for type[%s]", header
			      .getType().getClazz().getName()), e);
		}

	}

}
