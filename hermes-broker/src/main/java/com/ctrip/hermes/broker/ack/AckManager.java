package com.ctrip.hermes.broker.ack;

import java.util.Collection;

import com.ctrip.hermes.core.bo.Tpp;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckManager {

	void delivered(Tpp tpp, String groupId, boolean resend, Collection<Long> msgSeqs);

	void acked(Tpp tpp, String groupId, boolean resend, Collection<Long> msgSeqs);

	void nacked(Tpp tpp, String groupId, boolean resend, Collection<Long> msgSeqs);

}
