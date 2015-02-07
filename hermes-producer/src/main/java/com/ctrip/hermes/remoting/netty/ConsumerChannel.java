package com.ctrip.hermes.remoting.netty;

import io.netty.channel.ChannelHandlerContext;

public class ConsumerChannel {

	private ChannelHandlerContext m_nettyCtx;

	private String m_topic;

	private String groupId;

	public ConsumerChannel(ChannelHandlerContext nettyCtx, String topic, String groupId) {
		m_nettyCtx = nettyCtx;
		m_topic = topic;
		this.groupId = groupId;
	}

	public ChannelHandlerContext getNettyCtx() {
		return m_nettyCtx;
	}

	public String getTopic() {
		return m_topic;
	}

	public String getGroupId() {
		return groupId;
	}

}
