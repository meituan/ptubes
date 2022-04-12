package com.meituan.ptubes.reader.container.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;

public class ConfigKeyConstants {

    public enum ServerConfiguration {
        TASKS("ptubes.server.tasks", true, null),
        DATA_SERVER_PORT("ptubes.server.dataport", false, "28332"),
        MONITOR_SERVER_PORT("ptubes.server.monitorport", false, "23333"),
        ;

        private static final Map<String, ServerConfiguration> KEY_MAP;
        static {
            KEY_MAP = new HashMap<>();
            for (ServerConfiguration sc : ServerConfiguration.values()) {
                KEY_MAP.put(sc.getKey(), sc);
            }
        }
        public static ServerConfiguration of(String key) {
            ServerConfiguration sc = KEY_MAP.get(key);
            if (Objects.isNull(sc)) {
                throw new IllegalArgumentException("No enum ServerConfiguration for key " + key);
            }
            return sc;
        }

        private String key;
        private boolean required;
        private String defaultValue;

        ServerConfiguration(String key, boolean required, String defaultValue) {
            this.key = key;
            this.required = required;
            this.defaultValue = defaultValue;
        }

        public String getKey() {
            return key;
        }

        public boolean isRequired() {
            return required;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }

    public enum TaskConfiguration {
        MYSQL_HOST("ptubes.reader.mysql.host", true, null),
        MYSQL_PORT("ptubes.reader.mysql.port", false, "3306"),
        MYSQL_USER("ptubes.reader.mysql.user", true, null),
        MYSQL_PASSWD("ptubes.reader.mysql.passwd", true, null),
        MYSQL_SUBSCRIPTION("ptubes.reader.mysql.subs", false, ""),
        STORAGE_MODE("ptubes.reader.storage.mode", false, "MIX"),
        STORAGE_MEM_BUFFERSIZE("ptubes.reader.storage.membuffersize", false, "524288000"),
        ;

        private static final Map<String, TaskConfiguration> KEY_MAP;
        static {
            KEY_MAP = new HashMap<>();
            for (TaskConfiguration tc : TaskConfiguration.values()) {
                KEY_MAP.put(tc.getKey(), tc);
            }
        }
        public static TaskConfiguration of(String key) {
            TaskConfiguration sc = KEY_MAP.get(key);
            if (Objects.isNull(sc)) {
                throw new IllegalArgumentException("No enum TaskConfiguration for key " + key);
            }
            return sc;
        }

        private String key;
        private boolean required;
        private String defaultValue;

        TaskConfiguration(String key, boolean required, String defaultValue) {
            this.key = key;
            this.required = required;
            this.defaultValue = defaultValue;
        }

        public String getKey() {
            return key;
        }

        public boolean isRequired() {
            return required;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public static String genConfigKey(String taskName, TaskConfiguration taskConfiguration) {
            return taskName + "@" + taskConfiguration.key;
        }

        public static Pair<String, TaskConfiguration> getTaskConfigKey(String key) {
            int delimiterIndex = key.indexOf("@");
            if (delimiterIndex == -1) {
                return null;
            } else {
                return Pair.of(key.substring(0, delimiterIndex), TaskConfiguration.of(key.substring(delimiterIndex + 1)));
            }
        }
    }

}
