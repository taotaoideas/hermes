package com.ctrip.hermes.range;

public interface Range extends Locatable {

    String getId();

    Offset startOffset();

    Offset endOffset();

    void updateEndOffset(Offset o);

    boolean contains(Offset o);

}
