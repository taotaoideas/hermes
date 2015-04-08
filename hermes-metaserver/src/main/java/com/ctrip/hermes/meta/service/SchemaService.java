package com.ctrip.hermes.meta.service;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Codec;

@Named
public class SchemaService {

	@Inject
	private MetaService m_meta;

	public Codec getCodec(String topic) {
		return m_meta.getCodec(topic);
	}
}
