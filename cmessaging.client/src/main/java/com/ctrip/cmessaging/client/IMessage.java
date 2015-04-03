package com.ctrip.cmessaging.client;

import com.ctrip.cmessaging.client.content.AckMode;

/**
 * @version build-time: 2014-4-30
 * @author Phxydown E-mail: 264162213@qq.com
 */
public interface IMessage {
	
	 public String getSubject();
	 
	 public String getExchangeName();
	 
	 public String getMessageID();
	 
	 public String getHeader();
	 
	 public byte[] getBody();
	 
	 public void setAcks(AckMode acks);
	 
	 public AckMode getAcks();
	 
	 public void dispose();
}
