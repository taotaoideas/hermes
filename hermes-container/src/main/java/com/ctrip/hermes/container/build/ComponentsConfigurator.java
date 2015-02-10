package com.ctrip.hermes.container.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.container.DefaultConsumerManager;
import com.ctrip.hermes.container.remoting.ConsumeRequestProcessor;
import com.ctrip.hermes.engine.ConsumerManager;
import com.ctrip.hermes.remoting.CommandProcessor;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(ConsumerManager.class, DefaultConsumerManager.class));

		all.add(C(CommandProcessor.class, ConsumeRequestProcessor.ID, ConsumeRequestProcessor.class) //
		      .req(ConsumerManager.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
