package com.meituan.ptubes.reader.storage.file.data.read;

import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;

public interface ReadDataManager<K, V> extends LifeCycle {

	void open(K dataKey) throws Exception;

	V next() throws Exception;

	K position();

}
