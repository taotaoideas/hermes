package com.ctrip.hermes.broker.queue.partition;


/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueuePartitionPuller {
	public void start();

	public interface ShutdownListener {
		public void onShutdown();
	}
}
