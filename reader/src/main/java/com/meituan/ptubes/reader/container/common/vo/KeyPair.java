package com.meituan.ptubes.reader.container.common.vo;


public class KeyPair {
	Object key;
	
	PtubesFieldType keyType;

	public PtubesFieldType getKeyType() {
		return keyType;
	}

	public Object getKey() {
		return key;
	}

	public KeyPair(Object key, PtubesFieldType keyType) {
		this.key = key;
		this.keyType = keyType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KeyPair keyPair = (KeyPair) o;

		if (!key.equals(keyPair.key)) {
			return false;
		}
		if (keyType != keyPair.keyType) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = key == null ? 0 : key.hashCode();
		result = 31 * result + keyType.hashCode();
		return result;
	}

	@Override
	public String toString() {
		
		return key + " | " + keyType;
	}
}
