package com.ctrip.hermes.meta.internal;

import com.ctrip.hermes.message.internal.StorageType;
import com.ctrip.hermes.meta.Meta;

public class LocalMetaLoader implements MetaLoader {

	public static final String ID = "local-meta-loader";

	@Override
	public Meta load(String topic) {
		Meta meta = new Meta();

		meta.setStorageType(StorageType.MEMORY.name());

		return meta;
	}

}
