package com.ctrip.hermes.broker.dal;

import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptorManager;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;

@Named(type = JdbcDataSourceDescriptorManager.class)
public class HermesJdbcDataSourceDescriptorManager extends JdbcDataSourceDescriptorManager {

	@Inject
	private MetaService m_metaService;

	@Override
	protected DataSourcesDef defineDatasources() {
		DataSourcesDef def = new DataSourcesDef();

		List<Datasource> dataSources = m_metaService.listMysqlDataSources();
		for (Datasource ds : dataSources) {
			Map<String, Property> dsProps = ds.getProperties();
			DataSourceDef dsDef = new DataSourceDef(ds.getId());
			PropertiesDef props = new PropertiesDef();

			props.setDriver("com.mysql.jdbc.Driver");
			if (dsProps.get("url") == null || dsProps.get("user") == null) {
				throw new IllegalArgumentException("url and user property can not be null in datasource definition " + ds);
			}
			props.setUrl(dsProps.get("url").getValue());
			props.setUser(dsProps.get("user").getValue());
			if (dsProps.get("password") != null) {
				props.setPassword(dsProps.get("password").getValue());
			}
			// TODO set other properties
			props.setConnectionProperties("useUnicode=true&autoReconnect=true");
			dsDef.setProperties(props);

			def.addDataSource(dsDef);
		}

		return def;
	}

}
