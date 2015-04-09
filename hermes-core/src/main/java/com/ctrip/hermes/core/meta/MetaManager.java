package com.ctrip.hermes.core.meta;

import com.ctrip.hermes.meta.entity.Meta;



public interface MetaManager {

	public Meta getMeta();

	public boolean updateMeta(Meta meta);
}
