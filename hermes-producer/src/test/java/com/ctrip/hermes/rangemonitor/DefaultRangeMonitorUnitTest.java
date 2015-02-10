package com.ctrip.hermes.rangemonitor;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultRangeMonitorUnitTest {

    private static final String ID = "DefaultRangeMonitorUnitTest";
    private final int LENGTH = 1000 * 10000; //最大是（一）千万级别，再多在buildRangeByBitmap中会出问题（遍历生成range超时）。


    /*@Test
    public void testCalculateRange1() {
        DefaultRangeMonitor monitor = new DefaultRangeMonitor();

        EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf(1,3,5,7,9);
        EWAHCompressedBitmap doneMap = EWAHCompressedBitmap.bitmapOf(1,3,7);
        EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf(3,7);


        List<Range> doneRanges = new ArrayList<>();
        List<Range> failedRanges = new ArrayList<>();
        List<Range> timeoutRanges = new ArrayList<>();

        sendMap = monitor.calculateRanges(sendMap, doneMap, failMap, doneRanges, failedRanges, timeoutRanges, ID);

        assertEquals(sendMap.toList().size(), 1);
        assertEquals(doneRanges.size(), 3);
        assertEquals(failedRanges.size(), 2);
        assertEquals(timeoutRanges.size(), 1);
    }

    @Test
    public void testCalculateRange2() {
        DefaultRangeMonitor monitor = new DefaultRangeMonitor();

        EWAHCompressedBitmap sendMap = EWAHCompressedBitmap.bitmapOf(1,2,3,4,10,11,12,13,14);
        EWAHCompressedBitmap doneMap = EWAHCompressedBitmap.bitmapOf(1,2,3,4,11,12);
        EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf();

        List<Range> doneRanges = new ArrayList<>();
        List<Range> failedRanges = new ArrayList<>();
        List<Range> timeoutRanges = new ArrayList<>();

        sendMap = monitor.calculateRanges(sendMap, doneMap, failMap, doneRanges, failedRanges, timeoutRanges, ID);

        assertEquals(sendMap.toList().size(), 2);
        assertEquals(doneRanges.size(), 2);
        assertEquals(failedRanges.size(), 0);
        assertEquals(timeoutRanges.size(), 1);
    }

    @Test
    public void testCalculateRangeWithBigData() {
        DefaultRangeMonitor monitor = new DefaultRangeMonitor();



        EWAHCompressedBitmap sendMap = buildByDateLength(LENGTH);
        EWAHCompressedBitmap doneMap = buildByDateLength(LENGTH -2);
        EWAHCompressedBitmap failMap = EWAHCompressedBitmap.bitmapOf(1,2);

        List<Range> doneRanges = new ArrayList<>();
        List<Range> failedRanges = new ArrayList<>();
        List<Range> timeoutRanges = new ArrayList<>();

        sendMap = monitor.calculateRanges(sendMap, doneMap, failMap, doneRanges, failedRanges, timeoutRanges, ID);

        assertEquals(sendMap.toList().size(), 2);
        assertEquals(doneRanges.size(), 1);
        assertEquals(failedRanges.size(), 1);
        assertEquals(timeoutRanges.size(), 0);
    }

    private EWAHCompressedBitmap buildByDateLength(int length) {
        EWAHCompressedBitmap temp = EWAHCompressedBitmap.bitmapOf();
        for (int i = 1;i <= length; i++) {
            temp.set(i);
        }
        return temp;
    }*/
}