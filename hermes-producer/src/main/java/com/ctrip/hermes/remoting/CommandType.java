package com.ctrip.hermes.remoting;

import java.util.HashMap;
import java.util.Map;

public enum CommandType {

	// TODO partition ids
	HandshakeRequest(1), HandshakeResponse(2), //
	SendMessageRequest(3), StartConsumerRequest(4), //
	ConsumeRequest(5), AckRequest(6), //
	SendMessageResponse(7);

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
