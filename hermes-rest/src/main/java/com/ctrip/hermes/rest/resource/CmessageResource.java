package com.ctrip.hermes.rest.resource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.ctrip.framework.clogging.agent.aggregator.impl.Aggregator;
import com.ctrip.framework.clogging.agent.aggregator.impl.Metrics;
import com.ctrip.framework.clogging.agent.log.ILog;
import com.ctrip.framework.clogging.agent.log.LogManager;
import com.ctrip.hermes.rest.common.MetricsConstant;
import com.ctrip.hermes.rest.service.CmessageTransferService;

@Path("/cmessage")
public class CmessageResource {

	private static ILog logger = LogManager.getLogger(CmessageResource.class);
	private static final Aggregator collectorMetricsAgg = Aggregator.getMetricsAggregator(60);
	private static AtomicInteger integer = new AtomicInteger(1);
	CmessageTransferService service = CmessageTransferService.getInstance();
	/**
	 * if you do post request via POSTMAN, remember to add Headers (Content-Type:application/json)
	 */
	@POST
	@Consumes("application/json")
	@Produces(MediaType.APPLICATION_JSON)
	public Integer getCollectorInfo(
			  Map<String, String> map) {

		metricsAddCount(MetricsConstant.CmessageReceive);

		String topic = map.get("topic");
		// TODO: in fact, content in map is byte[]...
		String content = map.get("content");
		String header = map.get("header");

		if (null == topic || null == content || null == header) {
			logger.error("Invalid Message: " + map);
			throw new RuntimeException("invalid message");
		}

		try {
			service.transfer(topic, content, header);
			metricsAddCount(MetricsConstant.CmessageDelivery);
		} catch (Exception e) {
		}
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
