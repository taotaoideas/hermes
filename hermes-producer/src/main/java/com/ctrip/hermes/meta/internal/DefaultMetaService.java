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
import com.ctrip.hermes.meta.transform.BaseVisitor2;

public class DefaultMetaService implements Initializable, MetaService {

	@Inject
	private MetaManager m_manager;

	private Meta m_meta;

	private Storage m_defaultStorage;

	private Map<String, Connector> m_connectors = new HashMap<String, Connector>();

	private Map<String, Storage> m_storages = new HashMap<String, Storage>();

	private Map<String, Storage> m_localStorages = new HashMap<String, Storage>();

	@Override
	public void initialize() throws InitializationException {
		m_meta = m_manager.getMeta();

		m_meta.accept(new MetaVisitor());
	}

	@Override
	public Connector getConnector(String topic) {
		if (m_meta.isDevMode()) {
			Connector localConnector = new Connector();
			localConnector.setType(Connector.LOCAL);
			return localConnector;
		} else {
			Connector connector = m_connectors.get(topic);
			if (connector == null) {
				throw new RuntimeException(String.format("Connector for topic %s is not found", topic));
			} else {
				return connector;
			}
		}
	}

	class MetaVisitor extends BaseVisitor2 {

		@Override
		public void visitConnectorChildren(Connector connector) {
			super.visitConnectorChildren(connector);
		}

		@Override
		public void visitMetaChildren(Meta meta) {
			super.visitMetaChildren(meta);
		}

		@Override
		public void visitPropertyChildren(Property property) {
			super.visitPropertyChildren(property);
		}

		@Override
		public void visitStorageChildren(Storage storage) {
			Connector connector = getAncestor(2);

			if (Connector.LOCAL.equals(connector.getType()) && storage.isDefault()) {
				m_defaultStorage = storage;
			}

			super.visitStorageChildren(storage);
		}

		@Override
		public void visitTopicChildren(Topic topic) {
			Storage storage = getAncestor(2);
			Connector connector = getAncestor(3);

			m_connectors.put(topic.getName(), connector);
			m_storages.put(topic.getName(), storage);

			if (Connector.LOCAL.equals(connector.getType())) {
				m_localStorages.put(topic.getName(), storage);
			}

			super.visitTopicChildren(topic);
		}

	}

	@Override
	public Storage getStorage(String topic) {
		Storage storage = null;
		if (m_meta.isDevMode()) {
			storage = m_localStorages.get(topic);
			if (storage == null) {
				storage = m_defaultStorage;
			}
		} else {
			storage = m_storages.get(topic);
		}

		if (storage == null) {
			throw new RuntimeException(String.format("Storage for topic %s is not found", topic));
		}

		return storage;
	}

}
