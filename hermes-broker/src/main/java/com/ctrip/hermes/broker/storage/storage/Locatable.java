package com.ctrip.hermes.broker.storage.storage;

public interface Locatable {

    public void setOffset(Offset offset);
    
    public Offset getOffset();
    
}
