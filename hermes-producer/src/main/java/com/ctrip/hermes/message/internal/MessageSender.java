package com.ctrip.hermes.message.internal;

import java.util.concurrent.Future;

import com.ctrip.hermes.channel.SendResult;
import com.ctrip.hermes.message.ProducerMessage;

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
