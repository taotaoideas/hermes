package com.ctrip.hermes.storage.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ctrip.hermes.storage.MessageQueue;
import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.pair.StoragePair;
import com.ctrip.hermes.storage.range.OffsetRecord;
import com.ctrip.hermes.storage.range.RangeEvent;
import com.ctrip.hermes.storage.range.RangeStatusListener;
import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public class StorageMessageQueue implements MessageQueue {

	private StoragePair<Record> m_msgPair;

	private StoragePair<Resend> m_resendPair;

	private Map<String, StoragePair<?>> m_id2Pair = new HashMap<String, StoragePair<?>>();

	private BlockingQueue<Resend> m_resendCache = new LinkedBlockingQueue<Resend>();

	public StorageMessageQueue(StoragePair<Record> main, StoragePair<Resend> resend) {
		m_msgPair = main;
		m_resendPair = resend;

		recordPair(main.getStorageIds(), main);
		recordPair(resend.getStorageIds(), resend);

		connectPairs(main, resend);
	}

	private void recordPair(List<String> storageIds, StoragePair<?> pair) {
		for (String id : storageIds) {
			m_id2Pair.put(id, pair);
		}
	}

	@Override
	public List<Record> read(int batchSize) throws StorageException {
		int remain = batchSize;

		List<Record> result = m_msgPair.readMain(batchSize);
		m_msgPair.waitForAck(result);
		remain -= result.size();

		if (remain > 0) {
			List<Resend> resends = new ArrayList<Resend>();

			while (remain > 0) {
				Resend cr = m_resendCache.poll();
				if (cr != null) {
					resends.add(cr);
					remain--;
				} else {
					break;
				}
			}

			if (remain > 0) {
				resends.addAll(m_resendPair.readMain(remain));
			}

			for (Resend resend : resends) {
				if (remain <= 0) {
					break;
				}

				if (resend.getDue() <= System.currentTimeMillis()) {

					Range r = resend.getRange();
					List<Record> resendMsgs = m_msgPair.readMain(r);

					for (Record msg : resendMsgs) {
						msg.setAckOffset(resend.getOffset());
					}

					m_resendPair.waitForAck(resendMsgs, resend.getOffset());

					result.addAll(resendMsgs);
					// TODO maybe more than batch size
					remain -= resendMsgs.size();
				} else {
					m_resendCache.offer(resend);
				}
			}
		}

		return result;
	}

	@Override
	public void write(List<Record> msgs) throws StorageException {
		m_msgPair.appendMain(msgs);
	}

	// TODO consumer with same groupId should connect to the same message queue
	// to mediate message dispatch
	@Override
	public void ack(List<OffsetRecord> records) throws StorageException {
		for (OffsetRecord rec : records) {
			StoragePair<?> pair = m_id2Pair.get(rec.getToUpdate().getId());
			if (pair == null) {
				throw new RuntimeException(rec.getToUpdate().getId() + " not found in " + m_id2Pair);
			}
			pair.ack(rec);
		}
	}

	@Override
	public void seek(Offset offset) {
		// TODO Auto-generated method stub

	}

	private void connectPairs(final StoragePair<Record> msgPair, final StoragePair<Resend> resendPair) {
		msgPair.addRangeStatusListener(new RangeStatusListener() {

			@Override
			public void onRangeSuccess(RangeEvent event) throws StorageException {
			}

			@Override
			public void onRangeFail(RangeEvent event) throws StorageException {
				// TODO
				long due = System.currentTimeMillis() + 1000;
				resendPair.appendMain(new Resend(event.getRecord().getToBeDone(), due));
			}
		});

		resendPair.addRangeStatusListener(new RangeStatusListener() {

			@Override
			public void onRangeSuccess(RangeEvent event) throws StorageException {
			}

			@Override
			public void onRangeFail(RangeEvent event) throws StorageException {
				// TODO dead letter
				long due = System.currentTimeMillis() + 1000;
				resendPair.appendMain(new Resend(event.getRecord().getToBeDone(), due));
			}
		});
	}

	public StoragePair<Record> getMsgPair() {
		return m_msgPair;
	}

	public StoragePair<Resend> getResendPair() {
		return m_resendPair;
	}

}
