package com.ctrip.hermes.message.internal;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PartitioningAlgo {
	public int computePartitionNo(String key, int partitionCount);
}
