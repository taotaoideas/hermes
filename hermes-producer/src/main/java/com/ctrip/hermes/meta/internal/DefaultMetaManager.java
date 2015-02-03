package com.ctrip.hermes.meta.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.meta.Meta;
import com.ctrip.hermes.meta.MetaManager;

public class DefaultMetaManager extends ContainerHolder implements Initializable, MetaManager {

	private MetaLoader m_localMeta;

	private MetaLoader m_remoteMeta;

	@Override
	public Meta getMeta(String topic) {
		if (isLocalMode()) {
			return m_localMeta.load(topic);
		} else {
			return m_remoteMeta.load(topic);
		}
	}

	private boolean isLocalMode() {
		// TODO
		return true;
	}

	@Override
	public void initialize() throws InitializationException {
		m_localMeta = lookup(MetaLoader.class, LocalMetaLoader.ID);
		m_remoteMeta = lookup(MetaLoader.class, RemoteMetaLoader.ID);
	}

}
