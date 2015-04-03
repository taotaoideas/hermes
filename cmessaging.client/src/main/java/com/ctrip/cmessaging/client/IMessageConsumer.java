package com.ctrip.cmessaging.client;


/**
 * @version build-time: 2014-4-30
 * @author Phxydown E-mail: 264162213@qq.com
 */
public interface IMessageConsumer {
	
	public void topicBind(String topic, String exchangeName);
	
	public void setBatchSize(int batchSize);
	
	public void setIdentifier(String identifier);
}
