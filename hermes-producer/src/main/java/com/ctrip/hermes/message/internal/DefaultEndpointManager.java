package com.ctrip.hermes.message.internal;

import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultEndpointManager implements EndpointManager {

	@Inject
	private PartitioningAlgo m_partitionAlgo;

	@Inject
	private MetaService m_metaService;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.message.internal.EndpointManager#getEndpoint(java.lang.String, java.lang.String)
	 */
	@Override
	public Endpoint getEndpoint(String topic, String partition) {
		List<Partition> partitions = m_metaService.getPartitions(topic);
		return m_metaService.findEndpoint(m_partitionAlgo.compute(partition, partitions).getEndpoint());
	}
}
