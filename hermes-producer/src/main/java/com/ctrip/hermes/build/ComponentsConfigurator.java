package com.ctrip.hermes.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.initialization.Module;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesProducerModule;
import com.ctrip.hermes.channel.KafkaMessageChannelManager;
import com.ctrip.hermes.channel.LocalMessageChannelManager;
import com.ctrip.hermes.channel.LocalMessageQueueManager;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.channel.MessageQueueManager;
import com.ctrip.hermes.channel.MessageQueueMonitor;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.message.ProducerSinkManager;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.message.codec.internal.DefaultCodecManager;
import com.ctrip.hermes.message.codec.internal.JsonCodec;
import com.ctrip.hermes.message.internal.BrokerMessageSink;
import com.ctrip.hermes.message.internal.DefaultMessageSinkManager;
import com.ctrip.hermes.message.internal.MemoryMessageSink;
import com.ctrip.hermes.message.internal.ProducerPipeline;
import com.ctrip.hermes.message.internal.ProducerValveRegistry;
import com.ctrip.hermes.meta.MetaManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.internal.DefaultMetaManager;
import com.ctrip.hermes.meta.internal.DefaultMetaService;
import com.ctrip.hermes.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.meta.internal.MetaLoader;
import com.ctrip.hermes.meta.internal.RemoteMetaLoader;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.internal.DefaultProducer;
import com.ctrip.hermes.remoting.CommandCodec;
import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandProcessorManager;
import com.ctrip.hermes.remoting.CommandRegistry;
import com.ctrip.hermes.remoting.HandshakeResponseProcessor;
import com.ctrip.hermes.remoting.internal.DefaultCommandCodec;
import com.ctrip.hermes.remoting.internal.DefaultCommandRegistry;
import com.ctrip.hermes.remoting.netty.ClientManager;
import com.ctrip.hermes.remoting.netty.DefaultClientManager;
import com.ctrip.hermes.remoting.netty.NettyClient;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;
import com.ctrip.hermes.remoting.netty.NettyDecoder;
import com.ctrip.hermes.remoting.netty.NettyEncoder;
import com.ctrip.hermes.spi.Valve;
import com.ctrip.hermes.spi.internal.EncodeMessageValve;
import com.ctrip.hermes.spi.internal.TracingMessageValve;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	private final static String PRODUCER = "producer";

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(Module.class, HermesProducerModule.ID, HermesProducerModule.class));

		// meta
		all.add(C(MetaLoader.class, LocalMetaLoader.ID, LocalMetaLoader.class));
		all.add(C(MetaLoader.class, RemoteMetaLoader.ID, RemoteMetaLoader.class));
		all.add(C(MetaManager.class, DefaultMetaManager.class));
		all.add(C(MetaService.class, DefaultMetaService.class) //
		      .req(MetaManager.class));

		all.add(C(Producer.class, DefaultProducer.class) //
		      .req(Pipeline.class));
		all.add(C(Pipeline.class, ProducerPipeline.class) //
		      .req(ValveRegistry.class, PRODUCER) //
		      .req(ProducerSinkManager.class));

		// codecs
		all.add(C(Codec.class, JsonCodec.ID, JsonCodec.class));
		all.add(C(CodecManager.class, DefaultCodecManager.class));

		// sinks
		all.add(C(PipelineSink.class, MemoryMessageSink.ID, MemoryMessageSink.class) //
		      .req(MessageChannelManager.class, LocalMessageChannelManager.ID));
		all.add(C(PipelineSink.class, BrokerMessageSink.ID, BrokerMessageSink.class) //
		      .req(ClientManager.class));
		all.add(C(ProducerSinkManager.class, DefaultMessageSinkManager.class) //
		      .req(MetaService.class));

		// valves
		all.add(C(Valve.class, TracingMessageValve.ID, TracingMessageValve.class));
		all.add(C(Valve.class, EncodeMessageValve.ID, EncodeMessageValve.class) //
		      .req(CodecManager.class));
		all.add(C(ValveRegistry.class, PRODUCER, ProducerValveRegistry.class));

		all.add(C(ClientManager.class, DefaultClientManager.class));
		all.add(C(NettyClientHandler.class).is(PER_LOOKUP) //
		      .req(CommandProcessorManager.class));
		all.add(C(NettyClient.class).is(PER_LOOKUP));
		all.add(C(NettyDecoder.class).is(PER_LOOKUP) //
		      .req(CommandCodec.class));
		all.add(C(NettyEncoder.class).is(PER_LOOKUP) //
		      .req(CommandCodec.class));

		all.add(C(CommandProcessorManager.class) //
		      .req(CommandRegistry.class));
		all.add(C(CommandRegistry.class, DefaultCommandRegistry.class));
		all.add(C(CommandCodec.class, DefaultCommandCodec.class));

		// channel
		all.add(C(MessageChannelManager.class, LocalMessageChannelManager.ID, LocalMessageChannelManager.class) //
		      .req(MessageQueueManager.class));
		all.add(C(MessageQueueManager.class, LocalMessageQueueManager.class) //
		      .req(MetaService.class));
		all.add(C(MessageChannelManager.class, KafkaMessageChannelManager.ID, KafkaMessageChannelManager.class) //
		      .req(MetaService.class));

		// command processors
		all.add(C(CommandProcessor.class, HandshakeResponseProcessor.ID, HandshakeResponseProcessor.class));

		all.add(C(MessageQueueMonitor.class) //
		      .req(MessageQueueManager.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
