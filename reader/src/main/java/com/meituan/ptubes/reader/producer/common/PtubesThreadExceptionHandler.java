package com.meituan.ptubes.reader.producer.common;

public interface PtubesThreadExceptionHandler<DATA_TYPE> {

    void handleEventException(DATA_TYPE dataEvent, Throwable te, Object... args);

    void handlePrepareToStartupException(Throwable te, Object... args);

    void handlePrepareToShutdownException(Throwable te, Object... args);

    void handleInterruptException(InterruptedException ie, Object... args);

}
