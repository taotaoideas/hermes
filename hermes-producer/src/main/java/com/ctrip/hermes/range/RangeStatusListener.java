package com.ctrip.hermes.range;

public interface RangeStatusListener {

    // 返回range全部完成（无视失败）
    public void onRangeDone(Range range);

    // 返回失败的offset
    public void onRangeFail(Range range);

}
