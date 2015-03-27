package com.ctrip.hermes.core.partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PartitioningStrategy {
	public int computePartitionNo(String key, int partitionCount);
}
