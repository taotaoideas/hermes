package com.ctrip.hermes.meta.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.meta.server.MetaRestServer;
import com.ctrip.hermes.meta.service.SchemaService;
import com.ctrip.hermes.meta.service.TopicService;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(MetaRestServer.class));
		all.add(A(TopicService.class));
		all.add(A(SchemaService.class));

		all.addAll(new MetaDatabaseConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
