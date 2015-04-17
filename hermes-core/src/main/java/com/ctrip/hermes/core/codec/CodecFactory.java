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

	public static Codec getCodecByTopicName(String topic) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		com.ctrip.hermes.meta.entity.Codec codecEntity = metaService.getCodecByTopic(topic);
		return getCodec(codecEntity);
	}

	private static Codec getCodec(com.ctrip.hermes.meta.entity.Codec codecEntity) {
	   Codec codec = PlexusComponentLocator.lookup(Codec.class, codecEntity.getType());
		Map<String, String> configs = new HashMap<>();
		for (Property property : codecEntity.getProperties()) {
			configs.put(property.getName(), property.getValue());
		}
		codec.configure(configs);
		return codec;
   }

	public static Codec getCodecByType(String codecType) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);
		com.ctrip.hermes.meta.entity.Codec codecEntity = metaService.getCodecByType(codecType);
		return getCodec(codecEntity);
   }

}
