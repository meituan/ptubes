package com.meituan.ptubes.common.log;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.message.MessageFactory2;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.LocationAwareLogger;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.MessageFactory2Adapter;

public class Log4j2Impl implements Logger {

    private org.apache.logging.log4j.Logger log;

    private LoggerContext context = null;
    private MessageFactory2 messageFactory = null;

    private int logErrorCount;
    private int logWarnCount;
    private int logInfoCount;
    private int logDebugCount;

    public synchronized void init(boolean newContext) {
        if (!newContext) {
            context = Log4j2ContextFactory.getLoggerContextSingleInstance("log4j2.xml");
        } else {
            context = Log4j2ContextFactory.getLoggerContextNewInstance("log4j2.xml");
        }
    }

    /**
     * @since 0.2.21
     */
    public Log4j2Impl(String loggerName) {
        this(loggerName, true);
    }

    public Log4j2Impl(String loggerName, boolean newContext) {
        init(newContext);
        log = context.getLogger(loggerName);
        messageFactory = newFactory();
    }

    public Log4j2Impl(Class tClass) {
        this(tClass, true);
    }

    public Log4j2Impl(Class tClass, boolean newContext) {
        init(newContext);
        log = context.getLogger(tClass);
        messageFactory = newFactory();
    }

    public org.apache.logging.log4j.Logger getLog() {
        return log;
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
        String msg,
        Throwable e
    ) {

        logErrorCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.ERROR, e);
            return;
        }
        log.error(msg, e);
    }

    @Override
    public void error(String format, Object... arguments) {
        if (messageFactory != null && log instanceof LocationAwareLogger) {
            Message message = messageFactory.newMessage(format, arguments);
            locationLog(message, Level.ERROR, message.getThrowable());
        } else {
            error(String.format(format, arguments));
        }
    }

    @Override
    public void error(String msg) {
        logErrorCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.ERROR, null);
            return;
        }
        log.error(msg);
    }

    @Override
    public void debug(String msg) {
        logDebugCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.DEBUG, null);
            return;
        }
        log.debug(msg);
    }

    @Override
    public void debug(String format, Object... arguments) {
        if (messageFactory != null && log instanceof LocationAwareLogger) {
            Message message = messageFactory.newMessage(format, arguments);
            locationLog(message, Level.DEBUG, message.getThrowable());
        } else {
            debug(String.format(format, arguments));
        }
    }

    @Override
    public void warn(String format, Object... arguments) {
        if (messageFactory != null && log instanceof LocationAwareLogger) {
            Message message = messageFactory.newMessage(format, arguments);
            locationLog(message, Level.WARN, message.getThrowable());
        } else {
            warn(String.format(format, arguments));
        }
    }

    @Override
    public void debug(
        String msg,
        Throwable e
    ) {
        logDebugCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.DEBUG, e);
            return;
        }
        log.debug(
            msg,
            e
        );
    }

    @Override
    public void warn(String msg) {
        logWarnCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.WARN, null);
            return;
        }
        log.warn(msg);
    }

    @Override
    public void warn(
        String msg,
        Throwable e
    ) {
        logWarnCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.WARN, e);
            return;
        }

        log.warn(msg, e);
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isErrorEnabled();
    }

    @Override
    public int getLogWarnCount() {
        return logWarnCount;
    }


    @Override
    public int getLogErrorCount() {
        return logErrorCount;
    }

    @Override
    public void resetStat() {
        logErrorCount = 0;
        logWarnCount = 0;
        logInfoCount = 0;
        logDebugCount = 0;
    }

    @Override
    public int getLogDebugCount() {
        return logDebugCount;
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public void info(String format, Object... arguments) {
        if (messageFactory != null && log instanceof LocationAwareLogger) {
            Message message = messageFactory.newMessage(format, arguments);
            locationLog(message, Level.INFO, message.getThrowable());
        } else {
            info(String.format(format, arguments));
        }
    }

    @Override
    public void info(String msg) {
        logInfoCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.INFO, null);
            return;
        }
        log.info(msg);
    }

    @Override
    public void info(String msg, Throwable e) {
        logInfoCount++;
        if (this.log instanceof LocationAwareLogger) {
            locationLog(msg, Level.INFO, e);
            return;
        }

        log.info(msg, e);
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isEnabled(Level.WARN);
    }

    private void locationLog(String msg, Level level, Throwable throwable) {
        locationLog(new SimpleMessage(msg), level, throwable);
    }
    private void locationLog(Message msg, Level level, Throwable throwable) {
        if (log.isEnabled(level)) {
            StackTraceElement[] stackTraceElement = new Throwable().getStackTrace();
            StackTraceElement defaultResult = stackTraceElement[stackTraceElement.length - 1];
            for (int i =  0 ; i < stackTraceElement.length ; i++) {
                if (!stackTraceElement[i].getClassName().equals(this.getClass().getName())) {
                    defaultResult = stackTraceElement[i];
                    break;
                }
            }
            ((LocationAwareLogger) this.log).logMessage(
                    level,
                    null,
                    defaultResult.getClassName(),
                    defaultResult,
                    msg,
                    throwable
            );
        }
    }

    private MessageFactory2 newFactory() {
        try {
            MessageFactory result = AbstractLogger.DEFAULT_MESSAGE_FACTORY_CLASS.newInstance();
            if (result instanceof MessageFactory2) {
                return (MessageFactory2) result;
            }
            return new MessageFactory2Adapter(result);
        } catch (Throwable e) {
            error("init MessageFactory2 error. parameterized message type is not enable", e);
        }
        return null;
    }


    @Override
    public int getLogInfoCount() {
        return logInfoCount;
    }

    @Override
    public String toString() {
        return log.toString();
    }
}
