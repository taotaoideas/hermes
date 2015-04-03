package com.ctrip.cmessaging.client;

import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.impl.MessageHeader;

public interface IProducer {
	
	public void PublishAsync(String content, String subject) throws IllegalSubject;

	public void PublishAsync(String content, String subject, MessageHeader map) throws IllegalSubject;
	
}
