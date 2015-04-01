package com.ctrip.hermes.core.meta.internal;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	public static final String ID = "remote-meta-loader";

	@Override
	public Meta load() {
		// TODO Auto-generated method stub
		return null;
	}

}
