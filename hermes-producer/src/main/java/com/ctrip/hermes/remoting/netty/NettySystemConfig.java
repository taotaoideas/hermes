package com.ctrip.hermes.remoting.netty;

public class NettySystemConfig {
	public static boolean NettyPooledByteBufAllocatorEnable = //
	Boolean.parseBoolean(System.getProperty("a", "false"));

	public static int SocketSndbufSize = //
	Integer.parseInt(System.getProperty("a", "65535"));

	public static int SocketRcvbufSize = //
	Integer.parseInt(System.getProperty("a", "65535"));

	public static int ClientAsyncSemaphoreValue = //
	Integer.parseInt(System.getProperty("a", "2048"));

	public static int ClientOnewaySemaphoreValue = //
	Integer.parseInt(System.getProperty("a", "2048"));
}
