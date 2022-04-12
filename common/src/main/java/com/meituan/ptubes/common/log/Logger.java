
package com.meituan.ptubes.common.log;

public interface Logger {
    String getName();

    boolean isDebugEnabled();

    void debug(String msg);

    void debug(String format, Object... arguments);

    void debug(String msg, Throwable e);

    boolean isInfoEnabled();

    void info(String format, Object... arguments);

    void info(String msg);

    void info(String msg, Throwable e);

    boolean isWarnEnabled();

    void warn(String msg);

    void warn(String format, Object... arguments);

    void warn(String msg, Throwable e);

    boolean isErrorEnabled();

    void error(String msg, Throwable e);

    void error(String format, Object... arguments);

    void error(String msg);

    default void alarm(String msg, Throwable e) {
    }

    default void alarm(String format, Object... arguments) {
    }

    default void alarm(String msg) {
    }

    int getLogErrorCount();

    int getLogWarnCount();

    int getLogInfoCount();

    int getLogDebugCount();

    void resetStat();
}
