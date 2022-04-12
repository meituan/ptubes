package com.meituan.ptubes.reader.storage.manager.write;

import com.meituan.ptubes.reader.container.common.constants.StorageConstant;
import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import java.io.IOException;
import com.meituan.ptubes.reader.container.common.vo.BinlogInfo;
import org.apache.commons.lang3.tuple.Pair;


public interface WriteManager<K, V> extends LifeCycle {

	K append(V dataValue) throws Exception;

	void flush() throws IOException;

	K position();

	Pair<BinlogInfo, BinlogInfo> getStorageRange();

	StorageConstant.StorageMode getStorageMode();
}
