package com.ctrip.hermes.remoting.netty;

import io.netty.channel.Channel;

public interface ChannelEventListener {
	public void onChannelClose(Channel channel);
}