package com.ctrip.hermes.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.initialization.Module;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesProducerModule;
import com.ctrip.hermes.message.MessagePipeline;
import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.message.MessageSink;
import com.ctrip.hermes.message.MessageSinkManager;
import com.ctrip.hermes.message.codec.Codec;
import com.ctrip.hermes.message.codec.CodecManager;
import com.ctrip.hermes.message.codec.internal.DefaultCodecManager;
import com.ctrip.hermes.message.codec.internal.JsonCodec;
import com.ctrip.hermes.message.internal.DefaultMessagePipeline;
import com.ctrip.hermes.message.internal.DefaultMessageRegistry;
import com.ctrip.hermes.message.internal.DefaultMessageSinkManager;
import com.ctrip.hermes.message.internal.MemoryMessageSink;
import com.ctrip.hermes.meta.MetaManager;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.internal.DefaultMetaManager;
import com.ctrip.hermes.meta.internal.DefaultMetaService;
import com.ctrip.hermes.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.meta.internal.MetaLoader;
import com.ctrip.hermes.meta.internal.RemoteMetaLoader;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.producer.internal.DefaultProducer;
import com.ctrip.hermes.spi.MessageValve;
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
		      .req(MessagePipeline.class));
		all.add(C(MessagePipeline.class, DefaultMessagePipeline.class) //
		      .req(MessageRegistry.class, MessageSinkManager.class));

		// codecs
		all.add(C(Codec.class, JsonCodec.ID, JsonCodec.class));
		all.add(C(CodecManager.class, DefaultCodecManager.class));

		// sinks
		all.add(C(MessageSink.class, MemoryMessageSink.ID, MemoryMessageSink.class) //
		      .req(CodecManager.class));
		all.add(C(MessageSinkManager.class, DefaultMessageSinkManager.class) //
		      .req(MetaService.class));

		// valves
		all.add(C(MessageValve.class, TracingMessageValve.ID, TracingMessageValve.class));
		all.add(C(MessageRegistry.class, DefaultMessageRegistry.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
