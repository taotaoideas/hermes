package com.ctrip.hermes.core.meta.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type=MetaManager.class)
public class DefaultMetaManager extends ContainerHolder implements Initializable, MetaManager {

	private MetaLoader m_localMeta;

	private MetaLoader m_remoteMeta;

	@Override
	public Meta getMeta() {
		if (isLocalMode()) {
			return m_localMeta.load();
		} else {
			return m_remoteMeta.load();
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
