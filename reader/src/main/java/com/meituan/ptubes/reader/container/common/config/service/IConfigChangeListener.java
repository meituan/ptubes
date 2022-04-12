package com.meituan.ptubes.reader.container.common.config.service;


public interface IConfigChangeListener<T> {

    void onChange(T newConfig);

}
