package com.ctrip.hermes.storage.pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.storage.message.Message;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.range.RangeStatusListener;
import com.ctrip.hermes.storage.storage.Locatable;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;
import com.ctrip.hermes.storage.util.CollectionUtil;

public abstract class ClusteredPair<T extends Locatable> implements StoragePair<T> {

	protected List<? extends StoragePair<T>> m_childPairs;

	private Map<String, StoragePair<T>> m_id2Pair = new HashMap<String, StoragePair<T>>();

	public ClusteredPair(List<? extends StoragePair<T>> childPairs) {
		m_childPairs = childPairs;

		for (StoragePair<T> p : m_childPairs) {
			for (String id : p.getStorageIds()) {
				m_id2Pair.put(id, p);
			}
		}
	}

	@Override
	public List<T> readMain(int batchSize) throws StorageException {
		List<T> result = new ArrayList<>();

		int remain = batchSize;
		for (StoragePair<T> pair : m_childPairs) {
			if (remain <= 0) {
				break;
			}

			List<T> mainMsgs = pair.readMain(remain);
			remain -= mainMsgs.size();

			result.addAll(mainMsgs);
		}

		return result;
	}

	@Override
	public List<T> readMain(Range r) throws StorageException {
		return m_id2Pair.get(r.getId()).readMain(r);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void appendMain(List<T> payloads) throws StorageException {
		List<T>[] partitionedPayloads = new List[m_childPairs.size()];

		for (T payload : payloads) {
			int idx = findPair(payload);

			if (partitionedPayloads[idx] == null) {
				partitionedPayloads[idx] = new ArrayList<T>();
			}

			partitionedPayloads[idx].add(payload);
		}

		for (int i = 0; i < partitionedPayloads.length; i++) {
			List<T> subPayloads = partitionedPayloads[i];
			if (CollectionUtil.notEmpty(subPayloads)) {
				m_childPairs.get(i).appendMain(subPayloads);
			}
		}

	}

	@Override
	public void appendMain(T payload) throws StorageException {
		appendMain(Arrays.asList(payload));
	}

	@Override
	public void ack(OffsetRecord record) throws StorageException {
		m_id2Pair.get(record.getToUpdate().getId()).ack(record);
	}

	@Override
	public List<String> getStorageIds() {
		return new ArrayList<String>(m_id2Pair.keySet());
	}

	@Override
	public void waitForAck(List<Message> msgs) {
		if (CollectionUtil.notEmpty(msgs)) {
			m_id2Pair.get(CollectionUtil.first(msgs).getOffset().getId()).waitForAck(msgs);
		}
	}

	@Override
	public void waitForAck(List<Message> msgs, Offset offset) {
		if (CollectionUtil.notEmpty(msgs)) {
			m_id2Pair.get(CollectionUtil.first(msgs).getOffset().getId()).waitForAck(msgs, offset);
		}
	}

	@Override
	public void addRangeStatusListener(RangeStatusListener listener) {
		for (StoragePair<?> pair : m_childPairs) {
			pair.addRangeStatusListener(listener);
		}
	}

	protected abstract int findPair(T payloads);

}
