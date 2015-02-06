package com.ctrip.hermes.meta.internal;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.meta.MetaManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Connector;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.BaseVisitor;

public class DefaultMetaService implements Initializable, MetaService {

	@Inject
	private MetaManager m_manager;

	private Meta m_meta;

	private Storage m_defaultStorage;

	private Map<String, Connector> m_connectors = new HashMap<String, Connector>();

	private Map<String, Storage> m_storages = new HashMap<String, Storage>();

	@Override
	public void initialize() throws InitializationException {
		m_meta = m_manager.getMeta();

		m_meta.accept(new MetaVisitor());
	}

	@Override
	public String getConnectorType(String topic) {
		if (m_meta.isDevMode()) {
			return Connector.LOCAL;
		} else {
			Connector connector = m_connectors.get(topic);
			if (connector == null) {
				throw new RuntimeException(String.format("Connector for topic %s is not found", topic));
			} else {
				return connector.getType();
			}
		}
	}

	class MetaVisitor extends BaseVisitor {

		private Connector m_curConnector;

		private Storage m_curStorage;

		@Override
		public void visitConnector(Connector connector) {
			m_curConnector = connector;
			super.visitConnector(connector);
		}

		@Override
		public void visitMeta(Meta meta) {
			// TODO Auto-generated method stub
			super.visitMeta(meta);
		}

		@Override
		public void visitProperty(Property property) {
			// TODO Auto-generated method stub
			super.visitProperty(property);
		}

		@Override
		public void visitStorage(Storage storage) {
			m_curStorage = storage;

			if (Connector.LOCAL.equals(m_curConnector.getType()) && storage.isDefault()) {
				m_defaultStorage = storage;
			}

			super.visitStorage(storage);
		}

		@Override
		public void visitTopic(Topic topic) {
			m_connectors.put(topic.getName(), m_curConnector);
			m_storages.put(topic.getName(), m_curStorage);
			super.visitTopic(topic);
		}

	}

	@Override
	public String getStorageType(String topic) {
		Storage storage = m_storages.get(topic);

		if (storage == null) {
			storage = m_defaultStorage;
		}

		return storage.getType();
	}

}
