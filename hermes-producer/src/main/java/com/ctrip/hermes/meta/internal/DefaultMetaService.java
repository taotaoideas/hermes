package com.ctrip.hermes.meta.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.codec.CodecType;
import com.ctrip.hermes.meta.MetaManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.transform.BaseVisitor2;

public class DefaultMetaService implements Initializable, MetaService {

	@Inject
	private MetaManager m_manager;

	private Meta m_meta;

	private Map<String/* data source id */, Storage> m_dsId2Storage = new HashMap<>();

	@Override
	public void initialize() throws InitializationException {
		m_meta = m_manager.getMeta();

		m_meta.accept(new BaseVisitor2() {

			@Override
			protected void visitDatasourceChildren(Datasource ds) {
				Storage storage = getAncestor(2);
				m_dsId2Storage.put(ds.getId(), storage);

				super.visitDatasourceChildren(ds);
			}

		});
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.meta.MetaService#getPartitions(java.lang.String)
	 */
	@Override
	public List<Partition> getPartitions(String topic) {
		return m_meta.findTopic(topic).getPartitions();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.meta.MetaService#findEndpoint(java.lang.String)
	 */
	@Override
	public Endpoint findEndpoint(String endpointId) {
		return m_meta.findEndpoint(endpointId);
	}

	@Override
	public Storage findStorage(String topic) {
		Partition p0 = m_meta.findTopic(topic).getPartitions().get(0);
		return m_dsId2Storage.get(p0.getWriteDatasource());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.meta.MetaService#getCodecType(java.lang.String)
	 */
	@Override
	public CodecType getCodecType(String topic) {
		return CodecType.valueOf(m_meta.findTopic(topic).getCodec().getType().toUpperCase());
	}

}
