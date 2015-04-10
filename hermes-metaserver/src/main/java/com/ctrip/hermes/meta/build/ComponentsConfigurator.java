package com.ctrip.hermes.meta.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.meta.server.MetaRestServer;
import com.ctrip.hermes.meta.service.CodecService;
import com.ctrip.hermes.meta.service.SchemaService;
import com.ctrip.hermes.meta.service.ServerMetaManager;
import com.ctrip.hermes.meta.service.ServerMetaService;
import com.ctrip.hermes.meta.service.TopicService;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(MetaRestServer.class));
		all.add(A(TopicService.class));
		all.add(A(SchemaService.class));
		all.add(A(CodecService.class));
		
		all.add(A(ServerMetaManager.class));
		all.add(A(ServerMetaService.class));
		
		all.add(defineJdbcDataSourceConfigurationManagerComponent("/data/appdatas/hermes/datasources.xml"));
		all.addAll(new MetaDatabaseConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
