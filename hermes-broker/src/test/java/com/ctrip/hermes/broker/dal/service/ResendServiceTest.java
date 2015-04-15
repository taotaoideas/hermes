package com.ctrip.hermes.broker.dal.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.queue.mysql.service.ResendService;
import com.ctrip.hermes.core.bo.Tpp;

public class ResendServiceTest extends ComponentTestCase {

	private ResendService s;

	private String topic = "order_new";

	@Before
	public void before() {
		s = lookup(ResendService.class);
	}

	@Test
	public void test() throws Exception {
		List<ResendGroupId> resends = new ArrayList<>();
		s.write(resends);

		Tpp tpp = new Tpp(topic, 0, true);
		int groupId = 200;
		Date scheduleDate = new Date();
		s.read(tpp, groupId, scheduleDate, 10);
	}

}
