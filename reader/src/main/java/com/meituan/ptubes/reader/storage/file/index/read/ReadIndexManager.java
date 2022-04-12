package com.meituan.ptubes.reader.storage.file.index.read;

import com.meituan.ptubes.common.exception.GreaterThanStorageRangeException;
import com.meituan.ptubes.common.exception.LessThanStorageRangeException;
import com.meituan.ptubes.reader.container.common.lifecycle.LifeCycle;
import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;


public interface ReadIndexManager<K, V> extends LifeCycle {

	Pair<K, V> findOldest() throws IOException;

	Pair<K, V> findLatest() throws IOException;

	Pair<K, V> find(K indexKey) throws IOException, LessThanStorageRangeException, GreaterThanStorageRangeException;

}
