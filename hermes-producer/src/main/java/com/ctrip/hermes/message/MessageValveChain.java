package com.ctrip.hermes.message;

import java.util.List;

import com.ctrip.hermes.spi.MessageValve;

public class MessageValveChain {
	private MessageSink m_sink;

	private int m_index;

	private List<MessageValve> m_valves;

	public MessageValveChain(List<MessageValve> valves, MessageSink sink) {
		m_valves = valves;
		m_sink = sink;
	}

	public void handle(MessageContext ctx) {
		if (m_index < m_valves.size()) {
			MessageValve valve = m_valves.get(m_index);

			m_index++;
			valve.handle(this, ctx);
		} else {
			m_sink.handle(ctx);
		}
	}
}
