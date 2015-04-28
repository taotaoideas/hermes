package com.ctrip.hermes.core.component;

import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractLifeCycle implements LifeCycle {

	enum State {
		INIT, STARTING, STARTED, FAILED, STOPPING, STOPPED
	}

	private AtomicReference<State> m_state = new AtomicReference<>(State.INIT);

	@Override
	public synchronized void start() throws Exception {
		if (canStart()) {
			setStarting();
			try {
				doStart();
				setStarted();
			} catch (Throwable e) {
				// TODO log
				setFailed();
				throw e;
			}
		}
	}

	@Override
	public synchronized void stop() throws Exception {
		if (canStop()) {
			setStopping();
			try {
				doStop();
				setStopped();
			} catch (Throwable e) {
				// TODO log
				setFailed();
				throw e;
			}
		}
	}

	protected abstract void doStart() throws Exception;

	protected abstract void doStop();

	private void setStopped() {
		m_state.set(State.STOPPED);
	}

	private void setStopping() {
		m_state.set(State.STOPPING);
	}

	private void setFailed() {
		m_state.set(State.FAILED);
	}

	private void setStarted() {
		m_state.set(State.STARTED);
	}

	private void setStarting() {
		m_state.set(State.STARTING);
	}

	private boolean canStart() {
		return m_state.get() == State.INIT;
	}

	private boolean canStop() {
		return m_state.get() != State.STOPPING && m_state.get() != State.STOPPED;
	}

}
