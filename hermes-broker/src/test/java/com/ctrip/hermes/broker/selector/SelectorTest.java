package com.ctrip.hermes.broker.selector;

import java.io.IOException;

import org.junit.Test;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;

public class SelectorTest {

	@Test
	public void test() throws IOException {
		Selector s = new DefaultSelector();
		int partition = 0;
		final Tpg tpg = new Tpg("order_new", partition, "group1");
		Tpp tpp = new Tpp("order_new", partition, true);

		s.registerReadOp(tpg, new Runnable() {

			@Override
         public void run() {
				System.out.println("Read from " + tpg);
         }
			
		});
		s.updateWriteOffset(tpp, 11);
		s.select();

		System.in.read();
	}

}
