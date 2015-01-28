package com.ctrip.hermes.message;

import java.util.List;

import com.ctrip.hermes.spi.MessageValve;

public class MessageValveChain {
	private List<MessageValve> m_valves;

	public MessageValveChain(List<MessageValve> valves) {
		m_valves = valves;
	}

	public void handle(MessageContext ctx) {
		int index = ctx.getIndex();

		if (index < m_valves.size()) {
			MessageValve valve = m_valves.get(index);

			ctx.setIndex(index + 1);
			valve.handle(this, ctx);
		} else {
			ctx.getSink().handle(ctx);
		}
	}
}
