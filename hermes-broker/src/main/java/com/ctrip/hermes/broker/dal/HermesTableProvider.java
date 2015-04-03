package com.ctrip.hermes.broker.dal;

import java.util.Map;

import org.unidal.dal.jdbc.QueryDef;
import org.unidal.dal.jdbc.QueryEngine;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriority;
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
		MTopicPartitionPriority proto = (MTopicPartitionPriority) hints.get(QueryEngine.HINT_DATA_OBJECT);
		QueryDef def = (QueryDef) hints.get(QueryEngine.HINT_QUERY);
		QueryType queryType = def.getType();

		// TODO cache the result in meta service for better performance
		Partition p = m_metaService.findPartition(proto.getTopic(), proto.getPartition());

		switch (queryType) {
		case INSERT:
		case DELETE:
		case UPDATE:
			return p.getWriteDatasource();

		case SELECT:
			return p.getReadDatasource();

		default:
			throw new IllegalArgumentException(String.format("Unknown query type '%s'", queryType));
		}
	}

	@Override
	public String getPhysicalTableName(Map<String, Object> hints) {
		switch (m_table) {
		case "m-topic-partition-priority":
			MTopicPartitionPriority proto = (MTopicPartitionPriority) hints.get(QueryEngine.HINT_DATA_OBJECT);
			String fmt = "m_%s_%s_%s";
			return String.format(fmt, proto.getTopic(), proto.getPartition(), proto.getPriority());

		default:
			throw new IllegalArgumentException(String.format("Unknown logical table '%s'", m_table));
		}
	}

}
