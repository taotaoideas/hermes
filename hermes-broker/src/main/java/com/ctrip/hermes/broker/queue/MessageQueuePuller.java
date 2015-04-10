package com.ctrip.hermes.broker.queue;


/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePuller {
	public void start();

	public interface ShutdownListener {
		public void onShutdown();
	}
}
