package com.ctrip.hermes.storage.pair;

import java.util.List;

import com.ctrip.hermes.storage.message.Record;

public class ClusteredMessagePair extends ClusteredPair<Record> {

	private int m_priorities;

	public ClusteredMessagePair(List<MessagePair> childPairs) {
		super(childPairs);
		m_priorities = childPairs.size();
	}

	private int findSlot(int priority) {
		int defaultPriority = m_priorities - 1;

		priority = priority < 0 ? 0 : priority;
		priority = priority >= m_priorities ? defaultPriority : priority;

		return priority;
	}

	@Override
	protected int findPair(Record msg) {
		return findSlot(msg.getPriority());
	}

}
