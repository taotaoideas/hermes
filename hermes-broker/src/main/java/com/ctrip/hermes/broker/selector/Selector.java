package com.ctrip.hermes.broker.selector;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;

public interface Selector {

	void registerReadOp(Tpg tpg, Runnable runnable);

	void updateWriteOffset(Tpp tpp, long newWriteOffset);

	void select();

}
