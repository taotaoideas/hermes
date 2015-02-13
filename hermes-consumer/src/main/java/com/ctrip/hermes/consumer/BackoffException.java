package com.ctrip.hermes.consumer;

public class BackoffException extends Exception {

	private static final long serialVersionUID = 1L;

	public BackoffException() {
		super();
	}

	public BackoffException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public BackoffException(String message, Throwable cause) {
		super(message, cause);
	}

	public BackoffException(String message) {
		super(message);
	}

	public BackoffException(Throwable cause) {
		super(cause);
	}

}
