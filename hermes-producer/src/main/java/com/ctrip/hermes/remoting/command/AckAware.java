package com.ctrip.hermes.remoting.command;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckAware<T extends Ack> {
	public void onAck(T ack);
}
