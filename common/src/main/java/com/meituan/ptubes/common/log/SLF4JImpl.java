
package com.meituan.ptubes.common.log;

import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

public class SLF4JImpl implements Logger {
    private static final String ADAPTER_FQCN = SLF4JImpl.class.getName();
    private boolean isLocationAware;
    private int logErrorCount;
    private int logWarnCount;
    private int logInfoCount;
    private int logDebugCount;
    private org.slf4j.Logger log;

    public SLF4JImpl(String loggerName) {
        this(loggerName, true);
    }
    public SLF4JImpl(String loggerName, boolean newContext) {
        this.log = LoggerFactory.getLogger(loggerName);
        if (log instanceof LocationAwareLogger) {
            try {
                ((LocationAwareLogger) log).log(
                        null,
                        ADAPTER_FQCN,
                        LocationAwareLogger.DEBUG_INT,
                        "init slf4j logger",
                        null,
                        null
                );
                isLocationAware = true;
            } catch (Throwable t) {
                isLocationAware = false;
            }
        }
    }
    public SLF4JImpl(Class loggerName) {
        this(loggerName, true);
    }

    public SLF4JImpl(Class loggerName, boolean newContext) {
        this.log = LoggerFactory.getLogger(loggerName);
        if (log instanceof LocationAwareLogger) {
            try {
                ((LocationAwareLogger) log).log(
                        null,
                        ADAPTER_FQCN,
                        LocationAwareLogger.DEBUG_INT,
                        "init slf4j logger",
                        null,
                        null
                );
                isLocationAware = true;
            } catch (Throwable t) {
                isLocationAware = false;
            }
        }
    }

    @Override public String getName() {
        return log.getName();
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void error(
        String message,
        Throwable e
    ) {
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.ERROR_INT,
                message,
                null,
                e
            );
        } else {
            log.error(
                message,
                e
            );
        }
        logErrorCount++;
    }

    @Override
    public void error(String format, Object... arguments) {
        error(String.format(format, arguments));
    }

    @Override
    public void error(String message) {
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.ERROR_INT,
                message,
                null,
                null
            );
        } else {
            log.error(message);
        }
        logErrorCount++;
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public void info(String format, Object... arguments) {
        info(String.format(format, arguments));
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
    public void info(String message) {
        logInfoCount++;
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.INFO_INT,
                message,
                null,
                null
            );
        } else {
            log.info(message);
        }
    }

    @Override public void info(String message, Throwable e) {
        logInfoCount++;
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(null, ADAPTER_FQCN, LocationAwareLogger.INFO_INT, message, null, e);
        } else {
            log.info(message, e);
        }
    }

    @Override
    public void debug(String message) {
        logDebugCount++;
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.DEBUG_INT,
                message,
                null,
                null
            );
        } else {
            log.debug(message);
        }
    }

    @Override
    public void debug(
        String message,
        Throwable e
    ) {
        logDebugCount++;
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.DEBUG_INT,
                message,
                null,
                e
            );
        } else {
            log.debug(
                message,
                e
            );
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isWarnEnabled();
    }

    @Override
    public void warn(String message) {
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.WARN_INT,
                message,
                null,
                null
            );
        } else {
            log.warn(message);
        }
        logWarnCount++;
    }

    @Override
    public void warn(
        String message,
        Throwable e
    ) {
        if (isLocationAware) {
            ((LocationAwareLogger) log).log(
                null,
                ADAPTER_FQCN,
                LocationAwareLogger.WARN_INT,
                message,
                null,
                e
            );
        } else {
            log.warn(
                message,
                e
            );
        }
        logWarnCount++;
    }

    @Override public boolean isErrorEnabled() {
        return log.isErrorEnabled();
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
    public int getLogInfoCount() {
        return logInfoCount;
    }

    @Override
    public int getLogDebugCount() {
        return logDebugCount;
    }

    @Override
    public void resetStat() {
        logErrorCount = 0;
        logWarnCount = 0;
        logInfoCount = 0;
        logDebugCount = 0;
    }

}
