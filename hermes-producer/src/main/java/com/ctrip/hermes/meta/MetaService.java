package com.ctrip.hermes.meta;

import java.util.List;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;


public interface MetaService {

	String getEndpointType(String topic);

	/**
	 * @param topic
	 * @return
	 */
   List<Partition> getPartitions(String topic);

	/**
	 * @param endpointId
	 * @return
	 */
   Endpoint findEndpoint(String endpointId);
}
