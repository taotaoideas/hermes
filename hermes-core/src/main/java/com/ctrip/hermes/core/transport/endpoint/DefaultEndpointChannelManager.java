package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelConnectFailedEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EndpointChannelManager.class)
public class DefaultEndpointChannelManager implements EndpointChannelManager {

	@Inject
	private CommandProcessorManager m_cmdProcessorManager;

	private ConcurrentMap<Endpoint, EndpointChannel> channels = new ConcurrentHashMap<>();

	// TODO configable delay or with some strategy
	private int RECONNECT_DELAY_SECONDS = 1;

	@Override
	public EndpointChannel getChannel(Endpoint endpoint, EndpointChannelEventListener... listeners) {
		switch (endpoint.getType()) {
		case Endpoint.BROKER:
			if (!channels.containsKey(endpoint)) {
				synchronized (channels) {
					if (!channels.containsKey(endpoint)) {
						EndpointChannel channel = new NettyClientEndpointChannel(endpoint.getHost(), endpoint.getPort(),
						      m_cmdProcessorManager);

						channel.addListener(new NettyChannelAutoReconnectListener(channel, RECONNECT_DELAY_SECONDS));
						channel.addListener(listeners);
						channel.start();
						channels.put(endpoint, channel);
					}
				}
			}
			return channels.get(endpoint);

		default:
			throw new IllegalArgumentException(String.format("unknow endpoint type: %s", endpoint.getType()));
		}
	}

	protected static class NettyChannelAutoReconnectListener implements EndpointChannelEventListener {

		private int m_reconnectDelaySeconds;

		public NettyChannelAutoReconnectListener(EndpointChannel channel, int reconnectDelaySeconds) {
			m_reconnectDelaySeconds = reconnectDelaySeconds;
		}

		@Override
		public void onEvent(EndpointChannelEvent event) {
			if (event instanceof EndpointChannelConnectFailedEvent) {
				EventLoop eventLoop = event.getCtx();
				reconnect(eventLoop, event.getChannel());
			} else if (event instanceof EndpointChannelInactiveEvent) {
				ChannelHandlerContext ctx = event.getCtx();
				reconnect(ctx.channel().eventLoop(), event.getChannel());
			}
		}

		private void reconnect(EventLoop eventLoop, final EndpointChannel channel) {
			eventLoop.schedule(new Runnable() {

				@Override
				public void run() {
					// TODO log
					System.out.println("Reconnect...");
					channel.start();
				}
			}, m_reconnectDelaySeconds, TimeUnit.SECONDS);
		}

	}
}
