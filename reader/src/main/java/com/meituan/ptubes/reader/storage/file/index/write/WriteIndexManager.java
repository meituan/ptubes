package com.meituan.ptubes.reader.storage.file.index.write;

import com.meituan.ptubes.reader.storage.common.DataPosition;
import java.io.IOException;
import java.util.Date;

public interface WriteIndexManager<K, V> {
	void append(K indexKey, V indexValue) throws Exception;

	void flush() throws IOException;

	void clean(Date date) throws IOException;

	DataPosition position();
}
