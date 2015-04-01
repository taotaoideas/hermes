package com.ctrip.hermes.storage.storage.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.unidal.tuple.Pair;

public class SimpleConsumerUtil {
	static int DEFAULT_BUFFER_SIZE = 100;

	static int DEFAULT_FINDLEADER_RETRY = 3;

	static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			throw new RuntimeException("Error fetching data Offset Data the Broker. Reason: "
			      + response.errorCode(topic, partition));
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	static String findNewLeader(String oldLeader, List<Pair<String, Integer>> seedBrokers, String topic, int partition) {
		for (int i = 0; i < DEFAULT_FINDLEADER_RETRY; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(seedBrokers, topic, partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover, or it was a non-Broker issue
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		throw new RuntimeException("Unable to find new leader after Broker failure. Exiting");
	}

	static TopicMetadataResponse getTopicMetadata(String host, Integer port, String topic) {
		TopicMetadataResponse resp = null;
		SimpleConsumer consumer = null;
		try {
			consumer = new SimpleConsumer(host, port, DEFAULT_BUFFER_SIZE, 64 * 1024, "topicLookup");
			List<String> topics = Collections.singletonList(topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			resp = consumer.send(req);
		} finally {
			if (consumer != null)
				consumer.close();
		}
		return resp;
	}

	static PartitionMetadata findLeader(List<Pair<String, Integer>> seedBrokers, String topic, int partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (Pair<String, Integer> seed : seedBrokers) {
			try {
				TopicMetadataResponse resp = getTopicMetadata(seed.getKey(), seed.getValue(), topic);

				for (TopicMetadata item : resp.topicsMetadata()) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
				      + ", " + partition + "] Reason: " + e);
			}
		}
		if (returnMetaData != null) {
			seedBrokers.clear();
			for (Broker replica : returnMetaData.replicas()) {
				seedBrokers.add(new Pair<String, Integer>(replica.host(), replica.port()));
			}
		}
		return returnMetaData;
	}
}
