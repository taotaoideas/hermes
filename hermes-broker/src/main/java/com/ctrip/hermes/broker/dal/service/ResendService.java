package com.ctrip.hermes.broker.dal.service;

import java.util.Date;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.dal.hermes.OffsetResend;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendEntity;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdDao;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdEntity;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;

@Named(type = ResendService.class)
public class ResendService {

	@Inject
	private ResendGroupIdDao m_resendDao;

	@Inject
	private OffsetResendDao m_offsetDao;

	public void write(List<ResendGroupId> resends) throws DalException {
		m_resendDao.insert(resends.toArray(new ResendGroupId[resends.size()]));
	}

	public List<ResendGroupId> read(Tpp tpp, int groupId, Date scheduleDate, int batchSize) throws DalException {
		return m_resendDao.find(tpp.getTopic(), tpp.getPartitionNo(), groupId, scheduleDate, batchSize,
		      ResendGroupIdEntity.READSET_FULL);
	}

	public void findLastOffset() {

	}

	public void updateOffset(OffsetResend offset, Date newLastScheduleDate, long newLastId) throws DalException {
		offset.setLastScheduleDate(newLastScheduleDate);
		offset.setLastId(newLastId);
		m_offsetDao.updateByPK(offset, OffsetResendEntity.UPDATESET_OFFSET);
	}

}
