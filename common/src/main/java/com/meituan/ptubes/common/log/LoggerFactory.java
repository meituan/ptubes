
package com.meituan.ptubes.common.log;

import com.meituan.ptubes.common.exception.PtubesException;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("rawtypes")
public class LoggerFactory {

    public static final Map<String, Logger> TASK_NAME_TO_LOG = new ConcurrentHashMap<>();

    public static final Map<Object, Logger> LOGGER_NAME_TO_LOG = new ConcurrentHashMap<>();

    // private static String DEFAULT_LOG_TYPE_PROPERTY = "ptubes.client.log.type";
    public static final String DEFAULT_LOG_TYPE_PROPERTY = "ptubes.sdk.log.type";

    // private static String DEFAULT_LOG_DIR_PROPERTY = "ptubes.client.log.dir";
    public static final String DEFAULT_LOG_DIR_PROPERTY = "ptubes.sdk.log.dir";

    public static final String DEFAULT_LOG_DIR = "/data/applogs/ptubes";

    // private static String DEFAULT_LOG_FILENAME_PROPERTY = "ptubes.client.log.filename";
    public static final String DEFAULT_LOG_FILENAME_PROPERTY = "ptubes.sdk.log.filename";

    public static final String DEFAULT_LOG_FILENAME_FORMAT = "ptubes_sdk_%s.log";

    public static final String DEFAULT_TASK_NAME = "default";

    static {
        String dir = System.getProperty(DEFAULT_LOG_DIR_PROPERTY);
        if (dir == null || dir.length() == 0) {
            System.setProperty(
                DEFAULT_LOG_DIR_PROPERTY,
                DEFAULT_LOG_DIR
            );
        }
    }

    private static void initLogConstructorByLogger(Object loggerName) {
        Constructor logConstructor = null;

        // System.setProperty(DEFAULT_LOG_FILENAME_PROPERTY, String.format(DEFAULT_LOG_FILENAME_FORMAT, taskName));

        String logType = System.getProperty(DEFAULT_LOG_TYPE_PROPERTY);
        if (logType != null) {
            if ("slf4j".equalsIgnoreCase(logType)) {
                tryImplementationByLoggerName(
                    "org.slf4j.Logger",
                    "com.meituan.ptubes.common.log.SLF4JImpl",
                        loggerName
                );
            } else if ("log4j2".equalsIgnoreCase(logType)) {
                tryImplementationByLoggerName(
                    "org.apache.logging.log4j.Logger",
                    "com.meituan.ptubes.common.log.Log4j2Impl",
                        loggerName
                );
            } else if ("nolog".equalsIgnoreCase(logType)) {
                try {
                    logConstructor = NoLoggingImpl.class.getConstructor(String.class);
                } catch (Exception e) {
                    throw new IllegalStateException(
                        e.getMessage(),
                        e
                    );
                }
            }
        }

        tryImplementationByLoggerName(
                "org.apache.logging.log4j.Logger",
                "com.meituan.ptubes.common.log.Log4j2Impl",
                loggerName
        );

        if (LOGGER_NAME_TO_LOG.get(loggerName) == null) {
            try {
                logConstructor = NoLoggingImpl.class.getConstructor(String.class);
                LOGGER_NAME_TO_LOG.put(loggerName, (Logger)logConstructor.newInstance(LoggerFactory.class.getName()));
            } catch (Exception e) {
                throw new IllegalStateException(
                    e.getMessage(),
                    e
                );
            }
        }
    }

    private static boolean xmdlogExists() {
        try {
            Class.forName("com.meituan.inf.xmdlog.config.XMDConfiguration");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static void tryImplementationByLoggerName(
            String testClassName,
            String implClassName,
            Object loggerName
    ) {
        if (LOGGER_NAME_TO_LOG.containsKey(loggerName)) {
            return;
        }

        try {
            Resources.classForName(testClassName);
            Class implClass = Resources.classForName(implClassName);
            Constructor logConstructor = null;

            if (loggerName instanceof Class) {
                logConstructor = implClass.getConstructor(new Class[]{Class.class});
            } else if (loggerName instanceof String) {
                logConstructor = implClass.getConstructor(new Class[]{String.class});
            } else {
                throw new PtubesException("only Class or String type of logger is enable.");
            }

            Class<?> declareClass = logConstructor.getDeclaringClass();
            if (!Logger.class.isAssignableFrom(declareClass)) {
                logConstructor = null;
            }
            if (null != logConstructor) {
                Logger log = (Logger) logConstructor.newInstance(loggerName);
                LOGGER_NAME_TO_LOG.put(loggerName, log);
            }
        } catch (Throwable t) {
            // skip
            t.printStackTrace();
        }
    }

    private static synchronized void initLogConstructorByTaskName(String taskName) {
        Constructor logConstructor = null;

        System.setProperty(DEFAULT_LOG_FILENAME_PROPERTY, String.format(DEFAULT_LOG_FILENAME_FORMAT, taskName));

        String logType = System.getProperty(DEFAULT_LOG_TYPE_PROPERTY);
        if (logType != null) {
            if ("slf4j".equalsIgnoreCase(logType)) {
                tryImplementationByTaskName(
                        "org.slf4j.Logger",
                        "com.meituan.ptubes.common.log.SLF4JImpl",
                        taskName
                );
            } else if ("log4j2".equalsIgnoreCase(logType)) {
                tryImplementationByTaskName(
                        "org.apache.logging.log4j.Logger",
                        "com.meituan.ptubes.common.log.Log4j2Impl",
                        taskName
                );
            } else if ("nolog".equalsIgnoreCase(logType)) {
                try {
                    logConstructor = NoLoggingImpl.class.getConstructor(String.class);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            e.getMessage(),
                            e
                    );
                }
            }
        }

        tryImplementationByTaskName(
                "org.apache.logging.log4j.Logger",
                "com.meituan.ptubes.common.log.Log4j2Impl",
                taskName
        );

        if (TASK_NAME_TO_LOG.get(taskName) == null) {
            try {
                logConstructor = NoLoggingImpl.class.getConstructor(String.class);
                TASK_NAME_TO_LOG.put(taskName, (Logger)logConstructor.newInstance(LoggerFactory.class.getName()));
            } catch (Exception e) {
                throw new IllegalStateException(
                        e.getMessage(),
                        e
                );
            }
        }
    }


    @SuppressWarnings("unchecked")
    private static void tryImplementationByTaskName(
        String testClassName,
        String implClassName,
        String taskName
    ) {
        if (TASK_NAME_TO_LOG.containsKey(taskName)) {
            return;
        }

        try {
            Resources.classForName(testClassName);
            Class implClass = Resources.classForName(implClassName);
            Constructor logConstructor = implClass.getConstructor(new Class[]{String.class});

            Class<?> declareClass = logConstructor.getDeclaringClass();
            if (!Logger.class.isAssignableFrom(declareClass)) {
                logConstructor = null;
            }

            if (null != logConstructor) {
                Logger log = (Logger) logConstructor.newInstance(LoggerFactory.class.getName());
                TASK_NAME_TO_LOG.put(taskName, log);
            }
        } catch (Throwable t) {
            // skip
            t.printStackTrace();
        }
    }

    /**
     * get a logger by logger name or class
     * @param clazz
     * @return
     */
    public static Logger getLogger(Class clazz) {
        if (LOGGER_NAME_TO_LOG.containsKey(clazz)) {
            return LOGGER_NAME_TO_LOG.get(clazz);
        } else {
            synchronized (LOGGER_NAME_TO_LOG) {
                initLogConstructorByLogger(clazz);
            }
            return LOGGER_NAME_TO_LOG.get(clazz);
        }
    }

    /**
     * get a logger by logger name or class
     * @param loggerName
     * @return
     */
    public static Logger getLogger(String loggerName) {
        if (LOGGER_NAME_TO_LOG.containsKey(loggerName)) {
            return LOGGER_NAME_TO_LOG.get(loggerName);
        } else {
            synchronized (LOGGER_NAME_TO_LOG) {
                initLogConstructorByLogger(loggerName);
            }
            return LOGGER_NAME_TO_LOG.get(loggerName);
        }
    }


    /**
     * get a logger by task name or class
     * @param clazz
     * @param taskName
     * @return
     */
    public static Logger getLoggerByTask(Class clazz, String taskName) {
        return getLoggerByTask(clazz.getName(), taskName);
    }

    /**
     * get a logger by task name or class
     * @param loggerName
     * @param taskName
     * @return
     */
    public static Logger getLoggerByTask(String loggerName, String taskName) {
        try {
            if (TASK_NAME_TO_LOG.containsKey(taskName)) {
                return TASK_NAME_TO_LOG.get(taskName);
            } else {
                synchronized (TASK_NAME_TO_LOG) {
                    initLogConstructorByTaskName(taskName);
                }
                return TASK_NAME_TO_LOG.get(taskName);
            }

        } catch (Throwable t) {
            throw new RuntimeException(
                "Error creating logger for logger '" + loggerName + "' with task " + taskName +".  Cause: " + t,
                t
            );
        }
    }
}
