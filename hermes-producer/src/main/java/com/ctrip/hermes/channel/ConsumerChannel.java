package com.ctrip.hermes.channel;

import java.util.List;

import com.ctrip.hermes.storage.range.OffsetRecord;

public interface ConsumerChannel {

	public void close();

	public void start(ConsumerChannelHandler handler);

	public void ack(List<OffsetRecord> recs);

}
