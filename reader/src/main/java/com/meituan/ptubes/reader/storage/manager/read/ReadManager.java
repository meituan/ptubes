package com.meituan.ptubes.reader.storage.manager.read;

import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import java.io.IOException;

public interface ReadManager<K, V> extends LifeCycle {
	K openOldest() throws IOException;

	K openLatest() throws IOException;

	K open(K dataKey) throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException;

	V next() throws IOException;

	K position();

	StorageConstant.StorageMode getStorageMode();

	// When mixing storage, return the currently read storage engine
	StorageConstant.StorageMode getCurrentStorageMode();

}
