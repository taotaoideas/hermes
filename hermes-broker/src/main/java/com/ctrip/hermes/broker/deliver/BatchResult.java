package com.ctrip.hermes.broker.deliver;

public class BatchResult {

	private EnumRange<?> m_failRange;

	private ContinuousRange<?> m_doneRange;

	public BatchResult(EnumRange<?> failRange, ContinuousRange<?> doneRange) {
		m_failRange = failRange;
		m_doneRange = doneRange;
	}

	public EnumRange<?> getFailRange() {
		return m_failRange;
	}

	public ContinuousRange<?> getDoneRange() {
		return m_doneRange;
	}

	public void merge(BatchResult resultToMerge) {
		EnumRange<?> failRangeToMerge = resultToMerge.getFailRange();
		if (m_failRange == null) {
			m_failRange = failRangeToMerge;
		} else {
			if (failRangeToMerge != null) {
				for (Long newOffset : failRangeToMerge.getOffsets()) {
					m_failRange.addOffset(newOffset);
				}
			}
		}

		ContinuousRange<?> doneRangeToMerge = resultToMerge.getDoneRange();
		long newDoneStart = Math.min(m_doneRange.getStart(), doneRangeToMerge.getStart());
		long newDoneEnd = Math.max(m_doneRange.getEnd(), doneRangeToMerge.getEnd());
		m_doneRange = new ContinuousRange<>(m_doneRange.getId(), newDoneStart, newDoneEnd);
	}

}
