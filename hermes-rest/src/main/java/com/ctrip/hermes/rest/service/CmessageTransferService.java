package com.ctrip.hermes.rest.service;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.rest.common.RestConstant;

public class CmessageTransferService {
	Producer p;

	private CmessageTransferService() {
		 p = Producer.getInstance();
	}

	private static class ServiceHodler {
		private static CmessageTransferService instance = new CmessageTransferService();

	}

	public static CmessageTransferService getInstance() {
		return ServiceHodler.instance;
	}

	public void transfer(String topic, String content, String header){

		// 由于开发初期的原因，全部放到order_new这个topic下
		Future<SendResult> future = p.message(RestConstant.CMESSAGEING_TOPIC, content)
				  .addProperty(RestConstant.CMESSAGING_ORIGIN_TOPIC, topic)
				  .addProperty(RestConstant.CMESSAGING_HEADER, header)
				  .send();
		// todo: handle the future: eg. the Exception in send().
	}


	public static void main(String[] args) {
		CmessageTransferService service = new CmessageTransferService();

		service.transfer("order_new", "content", "header");
	}
}
