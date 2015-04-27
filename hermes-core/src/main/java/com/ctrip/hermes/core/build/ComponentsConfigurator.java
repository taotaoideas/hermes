package com.ctrip.hermes.core.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesCoreModule;
import com.ctrip.hermes.core.codec.CMessagingCodec;
import com.ctrip.hermes.core.codec.JsonCodec;
import com.ctrip.hermes.core.env.DefaultClientEnvironment;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.meta.internal.ClientMetaManager;
import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.core.meta.internal.RemoteMetaLoader;
import com.ctrip.hermes.core.partition.HashPartitioningStrategy;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.command.processor.DefaultCommandProcessorRegistry;
import com.ctrip.hermes.core.transport.endpoint.DefaultEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.DefaultEndpointManager;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(HermesCoreModule.class));

		// partition algo
		all.add(A(HashPartitioningStrategy.class));

		// meta
		all.add(A(LocalMetaLoader.class));
		all.add(A(RemoteMetaLoader.class));
		all.add(A(ClientMetaManager.class));
		all.add(A(DefaultMetaService.class));

		// endpoint manager
		all.add(A(DefaultEndpointManager.class));

		// endpoint channel
		all.add(A(DefaultEndpointChannelManager.class));

		// command processor
		all.add(A(CommandProcessorManager.class));
		all.add(A(DefaultCommandProcessorRegistry.class));

		// codec
		all.add(A(DefaultMessageCodec.class));
		all.add(A(JsonCodec.class));
		all.add(A(CMessagingCodec.class));

		// env
		all.add(A(DefaultClientEnvironment.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
