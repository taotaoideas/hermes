package com.ctrip.hermes.message.codec.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;

public class DefaultCodecManager extends ContainerHolder implements Initializable, CodecManager {

	private Map<String, Codec> m_codecs = new HashMap<String, Codec>();

	@Override
	public Codec getCodec(String topic) {
		// support json codec only
		return m_codecs.get(JsonCodec.ID);
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, Codec> codecs = lookupMap(Codec.class);

		for (Entry<String, Codec> entry : codecs.entrySet()) {
			m_codecs.put(entry.getKey(), entry.getValue());
		}
	}

}
