
package com.meituan.ptubes.common.log;



public class NoLoggingImpl implements Logger {

    private int logInfoCount;
    private int logErrorCount;
    private int logWarnCount;
    private int logDebugCount;
    private String logName;

    public NoLoggingImpl(String logName) {
        this(logName, true);
    }

    public NoLoggingImpl(String loggerName, boolean newContext) {
        this.logName = logName;
    }

    public NoLoggingImpl(Class logName) {
        this(logName, true);
    }

    public NoLoggingImpl(Class logName, boolean newContext) {
        this.logName = logName.getName();
    }



    @Override
    public String getName() {
        return this.logName;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void error(
        String s,
        Throwable e
    ) {
        error(s);

        if (e != null) {
            e.printStackTrace();
        }
    }

    @Override
    public void error(String format, Object... arguments) {
        error(String.format(format, arguments));
    }

    @Override
    public void error(String s) {
        logErrorCount++;
        if (s != null) {
            System.err.println(logName + " : " + s);
        }
    }

    @Override
    public void debug(String s) {
        logDebugCount++;
    }

    @Override
    public void debug(
        String s,
        Throwable e
    ) {
        logDebugCount++;
    }

    @Override
    public void warn(String s) {
        logWarnCount++;
    }

    @Override
    public void warn(
        String s,
        Throwable e
    ) {
        logWarnCount++;
    }

    @Override public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public int getLogErrorCount() {
        return logErrorCount;
    }

    @Override
    public int getLogWarnCount() {
        return logWarnCount;
    }

    @Override
    public void resetStat() {
        logErrorCount = 0;
        logWarnCount = 0;
        logInfoCount = 0;
        logDebugCount = 0;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public void info(String format, Object... arguments) {
        info(String.format(format, arguments));
    }

    @Override
    public void info(String s) {
        logInfoCount++;
    }

    @Override
    public void info(String msg, Throwable e) {
        info(msg);

        if (e != null) {
            e.printStackTrace();
        }
    }

    @Override
    public void debug(String format, Object... arguments) {
        debug(String.format(format, arguments));
    }

    @Override
    public void warn(String format, Object... arguments) {
        warn(String.format(format, arguments));
    }
    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public int getLogInfoCount() {
        return logInfoCount;
    }

    @Override
    public int getLogDebugCount() {
        return logDebugCount;
    }

}
