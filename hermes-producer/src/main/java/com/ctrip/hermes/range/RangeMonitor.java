package com.ctrip.hermes.range;

public interface RangeMonitor {

    void startNewOffsets(Offset... offset);

    void offsetDone(Offset offset, boolean isAck);

    void addListener(RangeStatusListener listener);

}
