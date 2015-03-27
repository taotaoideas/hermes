package com.ctrip.hermes.core.transport.command;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum CommandType {
	MESSAGE_SEND(1, SendMessageCommand.class), //
	ACK_MESSAGE_SEND(2, SendMessageAckCommand.class), //
	;

	private static Map<Integer, CommandType> m_types = new HashMap<Integer, CommandType>();

	static {
		for (CommandType type : CommandType.values()) {
			m_types.put(type.getType(), type);
		}
	}

	public static CommandType valueOf(int type) {
		return m_types.get(type);
	}

	private int m_type;

	private Class<? extends Command> m_clazz;

	private CommandType(int type, Class<? extends Command> clazz) {
		m_type = type;
		m_clazz = clazz;
	}

	public int getType() {
		return m_type;
	}

	public Class<? extends Command> getClazz() {
		return m_clazz;
	}

}
