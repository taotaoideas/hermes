package com.ctrip.hermes.core.transport.endpoint.event;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class EndpointChannelEvent {
	protected Object ctx;

	public EndpointChannelEvent(Object ctx) {
		this.ctx = ctx;
	}

	@SuppressWarnings("unchecked")
	public <T> T getCtx() {
		return (T) ctx;
	}
}
