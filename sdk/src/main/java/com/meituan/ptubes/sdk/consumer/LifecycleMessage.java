package com.meituan.ptubes.sdk.consumer;

public class LifecycleMessage {
	public enum TypeId {
		START,
		SHUTDOWN,
		PAUSE,
		SUSPEND_ON_ERROR,
		RESUME
	}

	private Throwable lastError = null;
	private TypeId typeId = null;

	/** Creates a new START message */
	public static LifecycleMessage createStartMessage() {
		return (new LifecycleMessage()).switchToStart();
	}

	/** Creates a new SHUTDOWN message with no cause */
	public static LifecycleMessage createShutdownMessage() {
		return (new LifecycleMessage()).switchToShutdown(null);
	}

	/**
	 * Creates a new SHUTDOWN message with the specified reason
	 *
	 * @param reason
	 * 		the error that caused the shutdown
	 **/
	public static LifecycleMessage createShutdownMessage(Throwable reason) {
		return (new LifecycleMessage()).switchToShutdown(reason);
	}

	/** Creates a new PAUSE message */
	public static LifecycleMessage createPauseMessage() {
		return (new LifecycleMessage()).switchToPause();
	}

	/**
	 * Creates a new SUSPEND_ON_ERROR message with the specified reason
	 *
	 * @param reason
	 * 		the error that caused the suspend
	 */
	public static LifecycleMessage createSuspendOnErroMessage(Throwable reason) {
		return (new LifecycleMessage()).switchToSuspendOnError(reason);
	}

	/** Creates a new RESUME message */
	public static LifecycleMessage createResumeMessage() {
		return (new LifecycleMessage()).switchToResume();
	}

	/** Returns that caused the current state (SHUTDOWN or SUSPEND) */
	public Throwable getLastError() {
		return lastError;
	}

	/** Returns the current type of the message */
	public TypeId getTypeId() {
		return typeId;
	}

	/**
	 * Reuses the current object and switches it to a START message
	 *
	 * @return this message object
	 */
	public LifecycleMessage switchToStart() {
		typeId = TypeId.START;
		return this;
	}

	/**
	 * Reuses the current object and switches it to a SHUTDOWN message
	 *
	 * @param reason
	 * 		the error that caused the shutdown
	 * @return this message object
	 */
	public LifecycleMessage switchToShutdown(Throwable reason) {
		typeId = TypeId.SHUTDOWN;
		lastError = reason;
		return this;
	}

	/**
	 * Reuses the current object and switches it to a PAUSE message
	 *
	 * @return this message object
	 */
	public LifecycleMessage switchToPause() {
		typeId = TypeId.PAUSE;
		return this;
	}

	/**
	 * Reuses the current object and switches it to a new SUSPEND_ON_ERROR message
	 *
	 * @param reason
	 * 		the error that caused the suspend
	 * @return this message object
	 */
	public LifecycleMessage switchToSuspendOnError(Throwable reason) {
		typeId = TypeId.SUSPEND_ON_ERROR;
		lastError = reason;
		return this;
	}

	/**
	 * Reuses the current object and switches it to a RESUME message
	 *
	 * @return this message object
	 */
	public LifecycleMessage switchToResume() {
		typeId = TypeId.RESUME;
		return this;
	}

	@Override
	public String toString() {
		return typeId.toString();
	}
}
