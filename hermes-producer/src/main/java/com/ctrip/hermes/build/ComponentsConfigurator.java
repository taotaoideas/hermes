package com.ctrip.hermes.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.initialization.Module;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesProducerModule;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.message.Pipeline;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.message.codec.internal.DefaultCodecManager;
import com.ctrip.hermes.message.codec.internal.JsonCodec;
import com.ctrip.hermes.message.internal.BrokerMessageSink;
import com.ctrip.hermes.message.internal.DefaultMessagePipeline;
import com.ctrip.hermes.message.internal.DefaultMessageRegistry;
import com.ctrip.hermes.message.internal.DefaultMessageSinkManager;
import com.ctrip.hermes.message.internal.MemoryMessageSink;
import com.ctrip.hermes.message.internal.MessagePipelineSink;
import com.ctrip.hermes.message.internal.MessageValve;
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
import com.ctrip.hermes.remoting.netty.NettyClient;
import com.ctrip.hermes.remoting.netty.NettyClientHandler;
import com.ctrip.hermes.remoting.netty.NettyDecoder;
import com.ctrip.hermes.remoting.netty.NettyEncoder;
import com.ctrip.hermes.spi.internal.TracingMessageValve;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
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
		all.add(C(Pipeline.class, DefaultMessagePipeline.class) //
		      .req(ValveRegistry.class, "message") //
		      .req(MessageSinkManager.class));

		// codecs
		all.add(C(Codec.class, JsonCodec.ID, JsonCodec.class));
		all.add(C(CodecManager.class, DefaultCodecManager.class));

		// sinks
		all.add(C(MessagePipelineSink.class, MemoryMessageSink.ID, MemoryMessageSink.class) //
		      .req(CodecManager.class));
		all.add(C(MessagePipelineSink.class, BrokerMessageSink.ID, BrokerMessageSink.class) //
		      .req(CodecManager.class, ClientManager.class));
		all.add(C(MessageSinkManager.class, DefaultMessageSinkManager.class) //
		      .req(MetaService.class));

		// valves
		all.add(C(MessageValve.class, TracingMessageValve.ID, TracingMessageValve.class));
		all.add(C(ValveRegistry.class, "message", DefaultMessageRegistry.class));

		all.add(C(ClientManager.class));
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

		// command processors
		all.add(C(CommandProcessor.class, HandshakeResponseProcessor.ID, HandshakeResponseProcessor.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
