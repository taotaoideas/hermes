package com.ctrip.hermes.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.codec.AvroCodec;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.engine.KafkaConsumerBootstrap;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.pipeline.DefaultMessageSink;
import com.ctrip.hermes.producer.sender.KafkaMessageSender;
import com.ctrip.hermes.producer.sender.MessageSender;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();
		
		all.add(C(PipelineSink.class, Endpoint.KAFKA, DefaultMessageSink.class) //
		      .req(MessageSender.class, Endpoint.KAFKA));
		all.add(A(KafkaMessageSender.class));

		all.add(A(KafkaConsumerBootstrap.class));
		all.add(A(AvroCodec.class));
		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
