package com.ctrip.hermes;

import org.unidal.initialization.AbstractModule;
import org.unidal.initialization.Module;
import org.unidal.initialization.ModuleContext;
import org.unidal.lookup.annotation.Named;

@Named(type = Module.class, value = HermesCoreModule.ID)
public class HermesCoreModule extends AbstractModule {
	public static final String ID = "hermes-core";

	@Override
	public Module[] getDependencies(ModuleContext ctx) {
		return null;
	}

	@Override
	protected void execute(ModuleContext ctx) throws Exception {
	}

}
