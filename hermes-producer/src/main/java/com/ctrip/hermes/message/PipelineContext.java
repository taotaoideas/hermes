package com.ctrip.hermes.message;


public class PipelineContext<T> {

	private T m_message;

	public PipelineContext(T message) {
		m_message = message;
	}

	public T getMessage() {
		return m_message;
	}

}
