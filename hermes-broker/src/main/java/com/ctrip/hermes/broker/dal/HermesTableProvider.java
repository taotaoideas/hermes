package com.ctrip.hermes.broker.dal;

import java.util.Map;

import org.unidal.dal.jdbc.QueryDef;
import org.unidal.dal.jdbc.QueryEngine;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Partition;

public class HermesTableProvider implements TableProvider {

	@Inject
	private MetaService m_metaService;

	private String m_table;

	@Override
	public String getLogicalTableName() {
		return m_table;
	}

	@Override
	public String getDataSourceName(Map<String, Object> hints) {
		MTopicShardPriority proto = (MTopicShardPriority) hints.get(QueryEngine.HINT_DATA_OBJECT);
		QueryDef def = (QueryDef) hints.get(QueryEngine.HINT_QUERY);
		QueryType queryType = def.getType();

		// TODO cache the result in meta service for better performance
		Partition p = m_metaService.findPartition(proto.getTopic(), proto.getShard());

		switch (queryType) {
		case INSERT:
		case DELETE:
		case UPDATE:
			return p.getWriteDatasource();

		case SELECT:
			return p.getReadDatasource();

		default:
			throw new RuntimeException(String.format("Unknown query type '%s'", queryType));
		}
	}

	@Override
	public String getPhysicalTableName(Map<String, Object> hints) {
		MTopicShardPriority proto = (MTopicShardPriority) hints.get(QueryEngine.HINT_DATA_OBJECT);
		String fmt = "m_%s_%s_%s";
		return String.format(fmt, proto.getTopic(), proto.getShard(), proto.getPriority());
	}

}
