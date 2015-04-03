package com.ctrip.hermes.producer.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.partition.PartitioningStrategy;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.DefaultProducer;
import com.ctrip.hermes.producer.HermesProducerModule;
import com.ctrip.hermes.producer.pipeline.DefaultMessageSink;
import com.ctrip.hermes.producer.pipeline.DefaultProducerSinkManager;
import com.ctrip.hermes.producer.pipeline.ProducerPipeline;
import com.ctrip.hermes.producer.pipeline.ProducerValveRegistry;
import com.ctrip.hermes.producer.pipeline.TracingMessageValve;
import com.ctrip.hermes.producer.sender.BatchableMessageSender;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.ctrip.hermes.producer.sender.SimpleMessageSender;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(HermesProducerModule.class));

		all.add(A(DefaultProducer.class));
		all.add(A(ProducerPipeline.class));
		all.add(A(ProducerValveRegistry.class));

		// valves
		all.add(A(TracingMessageValve.class));

		// sinks
		all.add(A(DefaultProducerSinkManager.class)); 
		all.add(C(PipelineSink.class, Endpoint.BROKER, DefaultMessageSink.class) //
		      .req(MessageSender.class, Endpoint.BROKER)//
		);
		all.add(C(PipelineSink.class, Endpoint.LOCAL, DefaultMessageSink.class) //
		      .req(MessageSender.class, Endpoint.LOCAL)//
		);
		all.add(C(PipelineSink.class, Endpoint.TRANSACTION, DefaultMessageSink.class) //
		      .req(MessageSender.class, Endpoint.TRANSACTION)//
		);

		// message sender
		all.add(A(SimpleMessageSender.class));
		all.add(C(MessageSender.class, Endpoint.BROKER, BatchableMessageSender.class)//
		      .req(EndpointManager.class)//
		      .req(EndpointChannelManager.class)//
		      .req(PartitioningStrategy.class)//
		      .req(MetaService.class)//
		);
		all.add(C(MessageSender.class, Endpoint.TRANSACTION, BatchableMessageSender.class)//
		      .req(EndpointManager.class)//
		      .req(EndpointChannelManager.class)//
		      .req(PartitioningStrategy.class)//
		      .req(MetaService.class)//
		);

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
