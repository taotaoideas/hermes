package com.ctrip.hermes.broker.ack;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpp;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckManager {

	void delivered(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqs);

	void acked(Tpp tpp, String groupId, boolean resend, Map<Long, Integer> msgSeqs);

	void nacked(Tpp tpp, String groupId, boolean resend, Map<Long, Integer> msgSeqs);

}
