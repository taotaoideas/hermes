package com.ctrip.hermes.engine.notifier;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.engine.ConsumerContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerNotifier {

	void register(long correlationId, ConsumerContext consumerContext);

	void messageReceived(long correlationId, List<ConsumerMessage<?>> msgs);

	ConsumerContext find(long correlationId);

}
