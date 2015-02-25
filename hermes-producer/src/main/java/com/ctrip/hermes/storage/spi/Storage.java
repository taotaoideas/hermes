package com.ctrip.hermes.storage.spi;

import java.util.List;

import com.ctrip.hermes.storage.storage.Browser;
import com.ctrip.hermes.storage.storage.Range;
import com.ctrip.hermes.storage.storage.StorageException;

public interface Storage<T> {
    void append(List<T> payloads) throws StorageException;

    Browser<T> createBrowser(long offset);

    List<T> read(Range range) throws StorageException;
    
    T top() throws StorageException;
    
    String getId();

}
