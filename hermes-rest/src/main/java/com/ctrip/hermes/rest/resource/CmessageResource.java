package com.ctrip.hermes.rest.resource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.framework.clogging.agent.aggregator.impl.Aggregator;
import com.ctrip.framework.clogging.agent.aggregator.impl.Metrics;
import com.ctrip.framework.clogging.agent.log.ILog;
import com.ctrip.framework.clogging.agent.log.LogManager;
import com.ctrip.hermes.rest.common.MetricsConstant;
import com.ctrip.hermes.rest.common.RestConstant;
import com.ctrip.hermes.rest.service.CmessageTransferService;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

@Path("/cmessage")
public class CmessageResource {

	private static ILog logger = LogManager.getLogger(CmessageResource.class);
	private static final Aggregator collectorMetricsAgg = Aggregator.getMetricsAggregator(60);
	private static AtomicInteger integer = new AtomicInteger(0);
	CmessageTransferService service = CmessageTransferService.getInstance();

	/**
	 * if you do post request via POSTMAN, remember to add Headers (Content-Type:application/json)
	 */
	@POST
	@Consumes("application/json")
	@Produces(MediaType.APPLICATION_JSON)
	public Integer getCollectorInfo(
			  Map<String, String> map) {

		Transaction t = Cat.newTransaction(RestConstant.CAT_TYPE, RestConstant.CAT_NAME);

		metricsAddCount(MetricsConstant.CmessageReceive);

		String topic = map.get("topic");
		// TODO: in fact, content in map is byte[]...
		String content = map.get("content");
		String header = map.get("header");
		try {
			if (null == topic || null == content || null == header) {
				StringBuilder sb = new StringBuilder();
				sb.append("Invalid Message: ");
				sb.append(map);

				logger.error(sb.toString());
				Cat.logEvent(RestConstant.CAT_TYPE, RestConstant.CAT_NAME, Message.SUCCESS, sb.toString());
				throw new RuntimeException("invalid message");
			}

			service.transfer(topic, content, header);

			t.setStatus(Message.SUCCESS);
			metricsAddCount(MetricsConstant.CmessageDelivery);
		} catch (Exception e) {
			t.setStatus(e);
		}
		t.complete();

		Cat.logEvent(RestConstant.CAT_TYPE, RestConstant.CAT_NAME, Message.SUCCESS, map.toString());

		map.remove("content");
		logger.info("received cmessage message", content, map);

		return integer.addAndGet(1);
	}

	private void metricsAddCount(String cmessageReceive) {
		Metrics received = new Metrics(cmessageReceive);
		received.setCount(1);
		collectorMetricsAgg.add(received);
	}
}
