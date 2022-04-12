package com.meituan.ptubes.reader.storage.common.event;


public enum EventType {
	INSERT((byte)0),
	UPDATE((byte)1),
	DELETE((byte)2),
	DDL((byte)3),
	GTID((byte)4),
	COMMIT((byte)5),
	HEARTBEAT((byte)6),
	SENTINEL((byte)7),
	// error event
	NO_MORE_EVENT((byte)-1),
	NOT_IN_BUFFER((byte)-2),
	EXCEPTION_EVENT((byte)-3);


	private byte code;

	EventType(byte code) {
		this.code = code;
	}

	public byte getCode() {
		return code;
	}

	public static EventType getByCode(byte code) {
		for (EventType t : EventType.values()) {
			if (t.getCode() == code) {
				return t;
			}
		}
		return null;
	}

	public static boolean isBroadcastType(EventType eventType) {
		return eventType.equals(COMMIT) || eventType.equals(HEARTBEAT);
	}

	public static boolean isErrorEvent(EventType eventType) {
		return eventType.getCode() < 0;
	}

	public static boolean isControlEvent(EventType eventType) {
		return eventType.equals(COMMIT) || eventType.equals(HEARTBEAT) || eventType.equals(DDL);
	}

}
