package com.ctrip.hermes.core.policy.retry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class RetryPolicyFactory {

	public static RetryPolicy create(String policyValue) {
		if (policyValue != null) {
			if (policyValue.indexOf(":") != -1) {
				String[] splits = policyValue.split(":");
				if (splits != null && splits.length == 2) {
					String type = splits[0];
					String value = splits[1];

					if (type != null && !"".equals(type.trim()) && value != null && !"".equals(value.trim())) {

						switch (type.trim()) {
						case "1":
							return new FrequencySpecifiedRetryPolicy(value.trim());

						default:
							break;
						}
					}
				}
			}
		}
		throw new IllegalArgumentException(String.format("Unknown retry policy for value %s", policyValue));
	}
}
