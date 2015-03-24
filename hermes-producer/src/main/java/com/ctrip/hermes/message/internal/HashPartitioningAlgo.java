package com.ctrip.hermes.message.internal;

import java.util.Random;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class HashPartitioningAlgo implements PartitioningAlgo {

	private Random m_random = new Random();

	@Override
	public int computePartitionNo(String key, int partitionCount) {

		if (key == null) {
			return m_random.nextInt() % partitionCount;
		} else {
			return key.hashCode() % partitionCount;
		}
	}

}
