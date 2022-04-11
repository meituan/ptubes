package com.meituan.ptubes.reader.container.common.config.service;

import com.meituan.ptubes.common.exception.SchemaParseException;
import java.io.IOException;
import java.util.Map;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;

public interface ITableConfigChangeListener {

    void onChange(Map<String, TableConfig> newConfig) throws IOException, InterruptedException, SchemaParseException;
}
