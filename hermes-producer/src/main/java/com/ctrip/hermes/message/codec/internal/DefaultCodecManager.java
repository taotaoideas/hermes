package com.ctrip.hermes.message.codec.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.meta.MetaManager;

public class DefaultCodecManager extends ContainerHolder implements Initializable, CodecManager {

	@Inject
	MetaManager m_metaManager;

	private Map<CodecType, Codec> m_codecs = new HashMap<CodecType, Codec>();

	@Override
	public Codec getCodec(String topic) {
		return m_codecs.get(m_metaManager.getMeta(topic).getCodecType(topic));
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, Codec> codecs = lookupMap(Codec.class);

		for (Entry<String, Codec> entry : codecs.entrySet()) {
			m_codecs.put(CodecType.valueOf(entry.getKey()), entry.getValue());
		}
	}

}
