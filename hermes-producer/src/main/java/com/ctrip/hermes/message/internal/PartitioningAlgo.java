package com.ctrip.hermes.message.internal;

import java.util.List;

import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PartitioningAlgo {
	public Partition compute(String key, List<Partition> partitions);
}
