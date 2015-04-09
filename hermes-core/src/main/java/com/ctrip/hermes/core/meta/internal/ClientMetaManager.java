package com.ctrip.hermes.core.meta.internal;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaManager.class, value = ClientMetaManager.ID)
public class ClientMetaManager extends ContainerHolder implements MetaManager {

	public static final String ID = "meta-client";

	@Inject(LocalMetaLoader.ID)
	private MetaLoader m_localMeta;

	@Inject(RemoteMetaLoader.ID)
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
		if (System.getenv().containsKey("isLocalMode")) {
			return Boolean.parseBoolean(System.getenv("isLocalMode"));
		}
		// FIXME for dev only
		return true;
	}

	@Override
	public boolean updateMeta(Meta meta) {
		if (isLocalMode()) {
			return m_localMeta.save(meta);
		} else {
			return m_remoteMeta.save(meta);
		}
	}

}
