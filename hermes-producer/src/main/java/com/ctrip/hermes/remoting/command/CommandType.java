package com.ctrip.hermes.remoting.command;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum CommandType {
	MESSAGE_SEND(1), //
	ACK_MESSAGE_SEND(2), //
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

	private CommandType(int type) {
		m_type = type;
	}

	public int getType() {
		return m_type;
	}

}
