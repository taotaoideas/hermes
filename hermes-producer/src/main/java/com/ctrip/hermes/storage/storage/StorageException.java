package com.ctrip.hermes.storage.storage;

// TODO should not be RuntimeException
public class StorageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StorageException() {
        super();
    }

    public StorageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(String message) {
        super(message);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }

}
