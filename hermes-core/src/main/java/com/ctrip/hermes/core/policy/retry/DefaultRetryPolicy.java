package com.ctrip.hermes.core.policy.retry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultRetryPolicy implements RetryPolicy {

	@Override
	public int getRetryTimes() {
		return 3;
	}

	@Override
	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis) {
		return currentTimeMillis + 15000;
	}

}
