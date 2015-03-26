package com.ctrip.hermes.partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PartitioningAlgo {
	public int computePartitionNo(String key, int partitionCount);
}
