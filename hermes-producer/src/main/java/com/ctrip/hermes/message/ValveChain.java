package com.ctrip.hermes.message;

import java.util.List;

import com.ctrip.hermes.spi.Valve;

public class ValveChain<T> {
	private PipelineSink<T> m_sink;

	private int m_index;

	private List<Valve<T>> m_valves;

	public ValveChain(List<Valve<T>> valves, PipelineSink<T> sink) {
		m_valves = valves;
		m_sink = sink;
	}

	public void handle(PipelineContext<T> ctx) {
		if (m_index < m_valves.size()) {
			Valve<T> valve = m_valves.get(m_index);

			m_index++;
			valve.handle(this, ctx);
		} else {
			m_sink.handle(ctx);
		}
	}
}
