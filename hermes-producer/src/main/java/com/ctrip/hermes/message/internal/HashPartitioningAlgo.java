package com.ctrip.hermes.message.internal;

import java.util.List;
import java.util.Random;

import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class HashPartitioningAlgo implements PartitioningAlgo {

	private Random m_random = new Random();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.message.internal.PartitioningAlgo#compute(java.lang.String, java.util.List)
	 */
	@Override
	public Partition compute(String key, List<Partition> partitions) {

		if (key == null) {
			return partitions.get(m_random.nextInt() % partitions.size());
		} else {
			return partitions.get(key.hashCode() % partitions.size());
		}
	}

}
