package com.meituan.ptubes.reader.storage.common.event;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.avro.util.Utf8;
import com.meituan.ptubes.common.exception.UnsupportedKeyException;


public class EventKey {
	public enum KeyType {
		LONG((byte)0),
		STRING((byte)1),
		SCHEMA((byte)2);

		private byte code;

		KeyType(byte code) {
			this.code = code;
		}

		public byte getCode() {
			return code;
		}

		public static KeyType getByCode(byte code) {
			for (KeyType t : KeyType.values()) {
				if (t.getCode() == code) {
					return t;
				}
			}
			return null;
		}
	}

	private final KeyType keyType;
	private final Long longKey;
	
	private final String stringKey;
	private final byte[] stringKeyInBytes;
	private final EventPart schemaKey;

	public EventKey(long key) {
		longKey = key;
		stringKey = null;
		keyType = KeyType.LONG;
		stringKeyInBytes = null;
		schemaKey = null;
	}

	/**
	 * @param key
	 * @deprecated Use the constructor with byte[] instead.
	 */
	@Deprecated
	public EventKey(String key) {
		stringKey = key;
		longKey = null;
		keyType = KeyType.STRING;
		stringKeyInBytes = null;
		schemaKey = null;
	}

	public EventKey(byte[] key) {
		keyType = KeyType.STRING;
		longKey = null;
		stringKey = null;
		stringKeyInBytes = Arrays.copyOf(key, key.length);
		schemaKey = null;
	}

	public EventKey(Object key) throws UnsupportedKeyException {
		if (key == null) {
			//      throw new IllegalArgumentException("Key cannot be null.");
			key = new Long(0);//Default primary key, to deal with extreme cases where pk is null
		}

		if ((key instanceof Long) || (key instanceof Integer)) {
			longKey = ((Number) key).longValue();
			stringKey = null;
			keyType = KeyType.LONG;
			stringKeyInBytes = null;
			schemaKey = null;
		} else if ((key instanceof String)) {
			longKey = null;
			stringKey = (String) key;
			keyType = KeyType.STRING;
			stringKeyInBytes = null;
			schemaKey = null;
		} else if ((key instanceof Utf8)) {
			longKey = null;
			stringKey = ((Utf8) key).toString();
			keyType = KeyType.STRING;
			stringKeyInBytes = null;
			schemaKey = null;
		} else if ((key instanceof byte[])) {
			longKey = null;
			stringKey = null;
			keyType = KeyType.STRING;
			stringKeyInBytes = Arrays.copyOf((byte[]) key, ((byte[]) key).length);
			schemaKey = null;
		} else if ((key instanceof EventPart)) {
			longKey = null;
			stringKeyInBytes = null;
			stringKey = null;
			schemaKey = (EventPart) key;
			keyType = KeyType.SCHEMA;
		} else {
			throw new UnsupportedKeyException("Bad getKey type: " + key.getClass().getName());
		}
	}

	public KeyType getKeyType() {
		return keyType;
	}

	public Long getLongKey() {
		return longKey;
	}

	/**
	 * @return
	 * @deprecated Use getStringKeyInBytes() instead.
	 * For now, use this API only if DbusEventKey is constructed with a string.
	 */
	public String getStringKey() {
		if (stringKey == null) {
			throw new RuntimeException("Invalid method invocation on getKey type " + keyType);
		}
		return stringKey;
	}

	/**
	 * Returns the getKey in a byte array.
	 * If the (deprecated) String-based constructor was used to construct the object
	 * then the UTF-8 representation of the string is returned.
	 */
	public byte[] getStringKeyInBytes() {
		if (stringKeyInBytes != null) {
            return Arrays.copyOf(stringKeyInBytes, stringKeyInBytes.length);
        }
		if (stringKey != null) {
            return stringKey.getBytes(Charset.forName("UTF-8"));
        }
		throw new RuntimeException("Invalid method invocation on getKey type " + keyType);
	}

	public EventPart getSchemaKey() {
		if (schemaKey == null) {
			throw new RuntimeException("Invalid method invocation on getKey type " + keyType);
		}
		return schemaKey;
	}

	public short getLogicalPartitionId() {
		return (short) Math.abs(hashCode() % Short.MAX_VALUE);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("DbusEventKey [_keyType=");
		builder.append(keyType);
		builder.append(", _longKey=");
		builder.append(longKey);
		builder.append(", _stringKey=");
		String stringKey;
		try {
			stringKey = this.stringKey != null ? this.stringKey : (stringKeyInBytes == null ? "NULL" : new String(
					stringKeyInBytes, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			stringKey = e.getLocalizedMessage();
		}
		builder.append(stringKey);
		builder.append(", _schemaKey=" + schemaKey);
		builder.append("]");
		return builder.toString();
	}

	// Used to generate the shard number, it is necessary to ensure that different readers hashcode is the same
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		if (keyType != null) {
			result = prime * result + keyType.getCode();
		}
		if (longKey != null) {
			result = prime * result + longKey.hashCode();
		}
		if (stringKey != null) {
			result = prime * result + stringKey.hashCode();
		} else if (stringKeyInBytes != null && stringKeyInBytes.length > 0) {
			result = prime * result + Arrays.hashCode(stringKeyInBytes);
		}
		if (schemaKey != null) {
			result = prime * result + schemaKey.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
            return true;
        }
		if (obj == null) {
            return false;
        }
		if (getClass() != obj.getClass()) {
            return false;
        }
		EventKey other = (EventKey) obj;
		if (keyType != other.keyType) {
            return false;
        }
		if (longKey == null) {
			if (other.longKey != null) {
                return false;
            }
		} else if (!longKey.equals(other.longKey)) {
            return false;
        }
		if (stringKey == null) {
			if (other.stringKey != null) {
                return false;
            }
		} else if (!stringKey.equals(other.stringKey)) {
            return false;
        }
		if (stringKeyInBytes == null) {
			if (other.stringKeyInBytes != null) {
                return false;
            }
		} else if (!Arrays.equals(stringKeyInBytes, other.stringKeyInBytes)) {
            return false;
        }
		if (schemaKey == null) {
			if (other.schemaKey != null) {
				return false;
			}
		} else {
			if (other.schemaKey == null) {
				return false;
			}
			if (!schemaKey.equals(other.schemaKey)) {
				return false;
			}
		}
		return true;
	}
}
