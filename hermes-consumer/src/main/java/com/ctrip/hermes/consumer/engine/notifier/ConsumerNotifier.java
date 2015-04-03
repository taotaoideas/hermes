package com.ctrip.hermes.consumer.engine.notifier;

import java.util.List;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerNotifier {

	void register(long correlationId, ConsumerContext consumerContext);

	void messageReceived(long correlationId, List<ConsumerMessage<?>> msgs);

	ConsumerContext find(long correlationId);

}
