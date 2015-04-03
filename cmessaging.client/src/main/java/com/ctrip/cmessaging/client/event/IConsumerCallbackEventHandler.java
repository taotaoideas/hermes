package com.ctrip.cmessaging.client.event;

import com.ctrip.cmessaging.client.IMessage;

/** 
 * @author Phxydown E-mail: 264162213@qq.com
 * @version build-time: 2014-5-5
 */
public interface IConsumerCallbackEventHandler {

	public void callback(IMessage message) throws Exception;
}
