package com.ctrip.hermes.broker.deliver;

import java.util.List;

public interface AckMonitor<T> {

	/**
    * @author Leo Liang(jhliang@ctrip.com)
    *
    */
   public interface Ack {

   }

	/**
    * @author Leo Liang(jhliang@ctrip.com)
    *
    */
   public interface Locatable {

   }

	void delivered(List<Locatable> locatables, T ctx);

	void acked(Locatable locatable, Ack ack);

	void addListener(AckStatusListener<T> listener);

}
