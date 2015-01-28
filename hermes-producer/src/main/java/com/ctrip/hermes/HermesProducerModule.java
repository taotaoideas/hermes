package com.ctrip.hermes;

import org.unidal.initialization.AbstractModule;
import org.unidal.initialization.Module;
import org.unidal.initialization.ModuleContext;

import com.ctrip.hermes.message.MessageRegistry;

public class HermesProducerModule extends AbstractModule {
	public static final String ID = "hermes-producer";

	@Override
	public Module[] getDependencies(ModuleContext ctx) {
		return null;
	}

	@Override
	protected void execute(ModuleContext ctx) throws Exception {
		MessageRegistry registry = ctx.lookup(MessageRegistry.class);

		registry.registerValve("trace", 2);
	}
}
