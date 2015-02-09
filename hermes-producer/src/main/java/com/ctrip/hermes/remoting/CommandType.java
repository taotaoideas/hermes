package com.ctrip.hermes.remoting;

import java.util.HashMap;
import java.util.Map;

public enum CommandType {

	HandshakeRequest(1), HandshakeResponse(2);

	private static Map<Integer, CommandType> m_types = new HashMap<Integer, CommandType>();

	static {
		for (CommandType type : CommandType.values()) {
			m_types.put(type.getType(), type);
		}
	}

	public static CommandType fromInt(int type) {
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
