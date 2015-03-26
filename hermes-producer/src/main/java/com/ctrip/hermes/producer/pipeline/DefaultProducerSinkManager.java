package com.ctrip.hermes.producer.pipeline;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.pipeline.PipelineSink;
import com.ctrip.hermes.producer.api.SendResult;
import com.ctrip.hermes.producer.pipeline.ProducerSinkManager;

public class DefaultProducerSinkManager extends ContainerHolder implements Initializable, ProducerSinkManager {

	@Inject
	private MetaService m_meta;

	private Map<String, PipelineSink<Future<SendResult>>> m_sinks = new HashMap<>();

	@Override
	public PipelineSink<Future<SendResult>> getSink(String topic) {
		String type = m_meta.getEndpointType(topic);

		if (Arrays.asList(Endpoint.BROKER, Endpoint.TRANSACTION, Endpoint.KAFKA, Endpoint.LOCAL).contains(type)) {
			return m_sinks.get(type);
		}else{
			throw new IllegalArgumentException(String.format("Unknown message sink for topic %s", topic));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void initialize() throws InitializationException {
		Map<String, PipelineSink> sinks = lookupMap(PipelineSink.class);

		for (Entry<String, PipelineSink> entry : sinks.entrySet()) {
			m_sinks.put(entry.getKey(), entry.getValue());
		}
	}
}
