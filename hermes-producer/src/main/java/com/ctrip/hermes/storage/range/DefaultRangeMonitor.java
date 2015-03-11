package com.ctrip.hermes.storage.range;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.ctrip.hermes.storage.storage.StorageException;

public class DefaultRangeMonitor implements RangeMonitor {

    private List<RangeStatusListener> m_listeners = new ArrayList<>();

    LinkedBlockingQueue<List<Long>> successQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<List<Long>> failQueue = new LinkedBlockingQueue<>();
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
                        List<Long> success = successQueue.take();
                        List<Long> fail = failQueue.take();

                        List<RangeEvent> successList = translator.buildContinuousRange(success);
                        List<RangeEvent> failList = translator.buildContinuousRange(fail);

                        if (successList.size() > 0 || failList.size() > 0) {
                            for (RangeStatusListener listener : m_listeners) {
                                for (RangeEvent event : successList) {
                                    listener.onRangeSuccess(event);
                                }
                                for (RangeEvent event : failList) {
                                    listener.onRangeFail(event);
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
