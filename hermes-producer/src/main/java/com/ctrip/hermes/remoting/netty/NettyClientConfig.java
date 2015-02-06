package com.ctrip.hermes.remoting.netty;

public class NettyClientConfig {
	// 处理Server Response/Request
	private int clientWorkerThreads = 4;

	private long connectTimeoutMillis = 3000;

	private int clientSocketSndBufSize = NettySystemConfig.SocketSndbufSize;

	private int clientSocketRcvBufSize = NettySystemConfig.SocketRcvbufSize;

	private boolean clientPooledByteBufAllocatorEnable = false;

	public int getClientWorkerThreads() {
		return clientWorkerThreads;
	}

	public void setClientWorkerThreads(int clientWorkerThreads) {
		this.clientWorkerThreads = clientWorkerThreads;
	}

	public long getConnectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	public void setConnectTimeoutMillis(long connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	public int getClientSocketSndBufSize() {
		return clientSocketSndBufSize;
	}

	public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
		this.clientSocketSndBufSize = clientSocketSndBufSize;
	}

	public int getClientSocketRcvBufSize() {
		return clientSocketRcvBufSize;
	}

	public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
		this.clientSocketRcvBufSize = clientSocketRcvBufSize;
	}

	public boolean isClientPooledByteBufAllocatorEnable() {
		return clientPooledByteBufAllocatorEnable;
	}

	public void setClientPooledByteBufAllocatorEnable(boolean clientPooledByteBufAllocatorEnable) {
		this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
	}
}
