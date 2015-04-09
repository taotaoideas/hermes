package com.ctrip.hermes.meta.service;

import java.util.Date;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.meta.dal.meta.MetaDao;
import com.ctrip.hermes.meta.dal.meta.MetaEntity;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaManager.class, value = ServerMetaManager.ID)
public class ServerMetaManager implements MetaManager {

	public static final String ID = "meta-server";

	@Inject
	private MetaDao m_metaDao;

	private Meta m_cachedMeta;

	@Override
	public Meta getMeta() {
		if (m_cachedMeta == null) {
			try {
				com.ctrip.hermes.meta.dal.meta.Meta dalMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
				m_cachedMeta = JSON.parseObject(dalMeta.getValue(), Meta.class);
			} catch (DalException e) {
				throw new RuntimeException("Get meta failed.", e);
			}
		}
		return m_cachedMeta;
	}

	@Override
	public boolean updateMeta(Meta meta) {
		if (m_cachedMeta != null && meta.hashCode() == m_cachedMeta.hashCode()) {
			return false;
		}
		
		com.ctrip.hermes.meta.dal.meta.Meta dalMeta = new com.ctrip.hermes.meta.dal.meta.Meta();
		try {
			dalMeta.setValue(JSON.toJSONString(meta));
			dalMeta.setLastModifiedTime(new Date(System.currentTimeMillis()));
			m_metaDao.insert(dalMeta);
		} catch (DalException e) {
			throw new RuntimeException("Update meta failed.", e);
		}
		m_cachedMeta = meta;
		return true;
	}

}
