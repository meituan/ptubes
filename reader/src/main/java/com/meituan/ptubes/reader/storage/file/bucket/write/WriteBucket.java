package com.meituan.ptubes.reader.storage.file.bucket.write;

import java.io.IOException;


public interface WriteBucket {
	void append(byte[] data) throws IOException;

	void flush() throws IOException;

	boolean hasRemainingForWrite(int needBytes);

	int position();
}
