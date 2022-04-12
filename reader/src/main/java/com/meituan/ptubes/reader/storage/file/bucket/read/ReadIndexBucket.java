package com.meituan.ptubes.reader.storage.file.bucket.read;

import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import java.io.IOException;

public interface ReadIndexBucket extends LifeCycle {
	byte[] next() throws IOException;

	void skip(long offset) throws IOException;

	int position();
}
