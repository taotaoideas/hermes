package com.ctrip.hermes.storage.storage;

public interface Range extends Locatable {

    String getId();

    Offset startOffset();

    Offset endOffset();

    boolean contains(Offset o);

}
