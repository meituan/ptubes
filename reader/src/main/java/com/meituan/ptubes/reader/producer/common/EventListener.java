package com.meituan.ptubes.reader.producer.common;

public interface EventListener<EVENT_TYPE> {

    boolean onEvent(EVENT_TYPE event) throws Exception;

//    void onBatchEvent(EVENT_TYPE events);

}
