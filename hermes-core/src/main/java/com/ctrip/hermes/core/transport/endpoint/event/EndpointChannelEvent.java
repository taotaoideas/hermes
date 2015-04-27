package com.ctrip.hermes.core.transport.endpoint.event;

import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class EndpointChannelEvent {
	protected Object m_ctx;

	protected EndpointChannel m_channel;

	public EndpointChannelEvent(Object ctx, EndpointChannel channel) {
		m_ctx = ctx;
		m_channel = channel;
	}

	@SuppressWarnings("unchecked")
	public <T> T getCtx() {
		return (T) m_ctx;
	}

	public EndpointChannel getChannel() {
		return m_channel;
	}

}
