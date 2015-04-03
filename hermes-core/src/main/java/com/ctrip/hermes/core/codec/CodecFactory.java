package com.ctrip.hermes.core.codec;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Property;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class CodecFactory {

	public static Codec getCodec(String topic) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		com.ctrip.hermes.meta.entity.Codec codecEntity = metaService.getCodec(topic);
		Codec codec = PlexusComponentLocator.lookup(Codec.class, codecEntity.getType());
		Map<String, String> configs = new HashMap<>();
		for (Property property : codecEntity.getProperties()) {
			configs.put(property.getName(), property.getValue());
		}
		codec.configure(configs);
		return codec;
	}

}
