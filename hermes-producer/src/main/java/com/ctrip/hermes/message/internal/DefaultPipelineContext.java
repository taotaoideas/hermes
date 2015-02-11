package com.ctrip.hermes.message.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.spi.Valve;

public class DefaultPipelineContext implements PipelineContext {

	private PipelineSink m_sink;

	private int m_index;

	private List<Valve> m_valves;

	private Object m_source;

	private Map<String, Object> m_attrs = new HashMap<String, Object>();

	public DefaultPipelineContext(List<Valve> valves, PipelineSink sink) {
		m_valves = valves;
		m_sink = sink;
	}

	@Override
	public void next(Object payload) {
		if (m_index == 0) {
			m_source = payload;
		}

		if (m_index < m_valves.size()) {
			Valve valve = m_valves.get(m_index);

			m_index++;
			valve.handle(this, payload);
		} else {
			m_sink.handle(this, payload);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getSource() {
		return (T) m_source;
	}

	@Override
	public void put(String name, String value) {
		m_attrs.put(name, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(String name) {
		return (T) m_attrs.get(name);
	}

}
