package com.ctrip.hermes.broker.deliver;


public interface AckStatusListener<T> {

	/**
    * @author Leo Liang(jhliang@ctrip.com)
    *
    */
   public interface Range {

   }

	public void onSuccess(Range range, T ctx);

	public void onFail(Range range, T ctx);

}
