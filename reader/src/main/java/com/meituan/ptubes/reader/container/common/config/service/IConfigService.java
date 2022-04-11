package com.meituan.ptubes.reader.container.common.config.service;

import com.meituan.ptubes.common.exception.PtubesException;


public interface IConfigService<T> {

    T getConfig();

    void setConfig(T config) throws InterruptedException, PtubesException;
}
