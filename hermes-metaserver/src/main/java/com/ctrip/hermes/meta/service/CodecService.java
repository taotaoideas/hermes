package com.ctrip.hermes.meta.service;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Codec;

@Named
public class CodecService {

	@Inject(ServerMetaService.ID)
	private MetaService m_metaService;

	public Codec getCodec(String topicName) {
		return m_metaService.getCodec(topicName);
	}

}
