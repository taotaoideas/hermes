package com.ctrip.hermes.core.message;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerConsumerMessage<T> extends BaseConsumerMessage<T> {

	private long offset;

	private DecodedMessage decodedMessage;

	@Override
	public void nack() {
		m_success = false;
	}

}
