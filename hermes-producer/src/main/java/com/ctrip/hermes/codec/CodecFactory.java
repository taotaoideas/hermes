package com.ctrip.hermes.codec;

import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class CodecFactory {

	public static Codec getCodec(String topic) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);
		
		CodecType codecType = metaService.getCodecType(topic);
		return PlexusComponentLocator.lookup(Codec.class, codecType.toString());
		
	}

}
