package com.ctrip.hermes.producer.sender;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageSender {

	/**
	 * @param msg
	 * @return
	 */
   Future<SendResult> send(ProducerMessage<?> msg);

}
