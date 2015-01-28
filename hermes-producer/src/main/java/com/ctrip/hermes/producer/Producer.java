package com.ctrip.hermes.producer;

public interface Producer {
	public Holder message(String topic, Object body);

	public interface Holder {
		public Holder withKey(String string);

		public void send();
	}
}
