package com.ctrip.cmessaging.client;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;


/**
 * @version build-time: 2014-4-30
 * @author Phxydown E-mail: 264162213@qq.com
 */
public interface ISyncConsumer extends IMessageConsumer {
	
	public IMessage consumeOne() throws ConsumeTimeoutException;
	
	
	public void setReceiveTimeout(long receiveTimeout);
	
}
