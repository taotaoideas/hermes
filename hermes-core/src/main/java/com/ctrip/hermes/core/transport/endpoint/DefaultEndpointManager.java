package com.ctrip.hermes.core.transport.endpoint;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EndpointManager.class)
public class DefaultEndpointManager implements EndpointManager {

	@Inject
	private MetaService m_metaService;

	@Override
	public Endpoint getEndpoint(String topic, int partitionNo) {
		return m_metaService.findEndpoint(m_metaService.getPartitions(topic).get(partitionNo).getEndpoint());
	}
}
