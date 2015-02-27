package com.ctrip.hermes.storage.storage.kafka;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.StoragePair;

public class KafkaGroup {

	public KafkaGroup(String topic, String groupId) {
	}

	public StoragePair<Message> createMessagePair() {
	   // TODO Auto-generated method stub
	   return null;
   }

	public StoragePair<Resend> createResendPair() {
	   // TODO Auto-generated method stub
	   return null;
   }

}
