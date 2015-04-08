package com.ctrip.hermes.core.meta;

import java.util.List;

import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

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

	Partition findPartition(String topic, int shard);

	/**
	 * @param topicPattern
	 * @return
	 */
   List<Topic> findTopicsByPattern(String topicPattern);

   /**
    * 
    * @param topic
    * @return
    */
   Codec getCodec(String topic);

	Topic findTopic(String topic);
}
