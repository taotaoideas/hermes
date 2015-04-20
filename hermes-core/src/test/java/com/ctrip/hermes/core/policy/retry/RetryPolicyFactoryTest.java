package com.ctrip.hermes.core.policy.retry;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class RetryPolicyFactoryTest {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid1() throws Exception {
		RetryPolicyFactory.create(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid2() throws Exception {
		RetryPolicyFactory.create("2");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid3() throws Exception {
		RetryPolicyFactory.create("2:");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid4() throws Exception {
		RetryPolicyFactory.create("2:  ");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid5() throws Exception {
		RetryPolicyFactory.create("   :1");
	}

	@Test
	public void testValid() throws Exception {
		RetryPolicy policy = RetryPolicyFactory.create("1:[1,2]");
		Assert.assertTrue(policy instanceof FrequencySpecifiedRetryPolicy);
		Assert.assertEquals(2, policy.getRetryTimes());
	}
}
