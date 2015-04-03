package com.ctrip.cmessaging.client;

import com.ctrip.cmessaging.client.event.IConsumerCallbackEventHandler;

/** 
 * @author Phxydown E-mail: 264162213@qq.com
 * @version build-time: 2014-5-8
 */
public interface IAsyncConsumer extends IMessageConsumer {
	
	public void addConsumerCallbackEventHandler(IConsumerCallbackEventHandler handler);

	public void ConsumeAsync();
	
	public void ConsumeAsync(int maxThread);
	
	public void ConsumeAsync(Boolean autoAck);
	
	public void ConsumeAsync(int maxThread, Boolean autoAck);
	
	public void stop();
}
