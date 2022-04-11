package com.meituan.ptubes.common.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.spi.LoggerContext;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Log4j2ContextFactory {
    private static final Map<String, LoggerContext> LOG_XML_PATH_TO_CONTEXT = new ConcurrentHashMap<>();

    public static LoggerContext getLoggerContextSingleInstance(String xmlFilePath) {
        if (LOG_XML_PATH_TO_CONTEXT.containsKey(xmlFilePath)) {
            return LOG_XML_PATH_TO_CONTEXT.get(xmlFilePath);
        } else {
            synchronized (Log4j2ContextFactory.class) {
                if (LOG_XML_PATH_TO_CONTEXT.containsKey(xmlFilePath)) {
                    return LOG_XML_PATH_TO_CONTEXT.get(xmlFilePath);
                }
                LoggerContext ctx = init(xmlFilePath);
                LOG_XML_PATH_TO_CONTEXT.put(xmlFilePath, ctx);
                return ctx;
            }
        }
    }

    public static LoggerContext getLoggerContextNewInstance(String xmlFilePath) {
        return init(xmlFilePath);
    }

    private static LoggerContext init(String xmlFilePath) {
        URL url = Log4j2Impl.class.getClassLoader()
                .getResource(xmlFilePath);
        LoggerContext ctx = null;
        if (url == null) {
            ctx = LogManager.getContext(false);
        } else {
            try {
                ctx = new org.apache.logging.log4j.core.LoggerContext(
                        "Rds-cdc-sdk",
                        null,
                        url.toURI()
                );
                ((org.apache.logging.log4j.core.LoggerContext) ctx).start();
            } catch (Exception e) {
                System.err.println("failed to initialize log4j2...");
                e.printStackTrace(System.err);
                ctx = LogManager.getContext(false);
            }
        }
        return ctx;
    }
}
