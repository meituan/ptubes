package com.meituan.ptubes.reader.storage.file.data.write;

import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import java.io.IOException;

public interface WriteDataManager<K, V> extends LifeCycle {

	K append(V dataValue) throws Exception;

	void flush() throws IOException;

	K position();
}
