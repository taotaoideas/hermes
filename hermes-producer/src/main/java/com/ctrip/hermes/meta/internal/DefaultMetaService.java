package com.ctrip.hermes.meta.internal;

import java.util.List;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.meta.MetaManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;

public class DefaultMetaService implements Initializable, MetaService {

	@Inject
	private MetaManager m_manager;

	private Meta m_meta;

	@Override
	public void initialize() throws InitializationException {
		m_meta = m_manager.getMeta();
	}

	@Override
	public String getEndpointType(String topic) {
		if (m_meta.isDevMode()) {
			return Endpoint.LOCAL;
		} else {
			String endpointId = m_meta.getTopics().get(topic).getPartitions().get(0).getEndpoint();
			Endpoint endpoint = m_meta.getEndpoints().get(endpointId);
			if (endpoint == null) {
				throw new RuntimeException(String.format("Endpoint for topic %s is not found", topic));
			} else {
				return endpoint.getType();
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.meta.MetaService#getPartitions(java.lang.String)
	 */
   @Override
   public List<Partition> getPartitions(String topic) {
	   return m_meta.findTopic(topic).getPartitions();
   }

	/* (non-Javadoc)
	 * @see com.ctrip.hermes.meta.MetaService#findEndpoint(java.lang.String)
	 */
   @Override
   public Endpoint findEndpoint(String endpointId) {
	   return m_meta.findEndpoint(endpointId);
   }

}
