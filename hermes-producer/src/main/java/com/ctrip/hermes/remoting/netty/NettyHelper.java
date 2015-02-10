package com.ctrip.hermes.remoting.netty;

import io.netty.channel.Channel;

import java.net.SocketAddress;

public class NettyHelper {

	public static String remoteAddr(final Channel channel) {
		if (channel == null) {
			return "";
		}
		
		SocketAddress remote = channel.remoteAddress();
		String addr = remote != null ? remote.toString() : "";

		if (addr.length() > 0) {
			int index = addr.lastIndexOf("/");
			if (index >= 0) {
				return addr.substring(index + 1);
			}

			return addr;
		}

		return "";
	}

}
