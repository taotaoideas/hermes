package com.ctrip.hermes.storage.range;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.storage.storage.Offset;
import com.ctrip.hermes.storage.storage.StorageException;

public class DefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<>();

    LinkedBlockingQueue<Triple<List<Long>, String, Offset>> successQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Triple<List<Long>, String, Offset>> failQueue = new LinkedBlockingQueue<>();
    NewOffsetBitmap bitmap = new NewOffsetBitmap(successQueue, failQueue);

    BitmapTranslator translator = new BitmapTranslator();

    public DefaultRangeMonitor() {
        runPullingThread();
    }

    private void runPullingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (; ; ) {
                    try {
                        Triple<List<Long>, String, Offset> success = successQueue.take();
                        Triple<List<Long>, String, Offset> fail = failQueue.take();

                        List<RangeEvent> successList = translator.buildContinuousRange(success);
                        List<RangeEvent> failList = translator.buildContinuousRange(fail);

                        if (successList.size() > 0 || failList.size() > 0) {
                            for (RangeStatusListener listener : m_listeners) {
                                for (RangeEvent event : failList) {     // send fail first
                                    listener.onRangeFail(event);
                                }
                                for (RangeEvent event : successList) {
                                    listener.onRangeSuccess(event);
                                }

                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    @Override
    public void startNewRange(OffsetRecord record) {
        translator.putOffset(bitmap, record);
    }

    @Override
    public void offsetDone(OffsetRecord record) throws StorageException {
        translator.ackOffset(bitmap, record);
    }

    @Override
    public void addListener(RangeStatusListener listener) {
        m_listeners.add(listener);
    }
}
