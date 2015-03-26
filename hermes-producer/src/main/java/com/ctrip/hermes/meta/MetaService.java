package com.ctrip.hermes.meta;

import java.util.List;

import com.ctrip.hermes.codec.CodecType;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;

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

	/**
	 * 
	 * @param topic
	 */
	Storage findStorage(String topic);

	/**
	 * @param topic
	 * @return
	 */
	CodecType getCodecType(String topic);
}
