
package com.meituan.ptubes.common.log;

/**
 * A class to simplify access to resources through the classloader.
 */

public final class Resources extends Object {
    private static ClassLoader defaultClassLoader;

    private Resources() {
    }

    public static Class<?> classForName(String className) throws ClassNotFoundException {
        Class<?> clazz = null;
        try {
            clazz = getClassLoader().loadClass(className);
        } catch (Exception e) {
            // Ignore
        }
        if (clazz == null) {
            clazz = Class.forName(className);
        }
        return clazz;
    }

    private static ClassLoader getClassLoader() {
        if (defaultClassLoader != null) {
            return defaultClassLoader;
        } else {
            return Thread.currentThread()
                .getContextClassLoader();
        }
    }

}
