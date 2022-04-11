package com.meituan.ptubes.reader.container.common.config;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.exception.InvalidConfigException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.common.utils.StringUtil;
import com.meituan.ptubes.reader.container.config.ConfigKeyConstants;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class FileConfigService extends AbstractConfigService {
    private static final Logger LOG = LoggerFactory.getLogger(FileConfigService.class);

    private static final ConfigService INSTANCE = new FileConfigService();
    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static Properties serverProperties;
    private static Map<String/* taskName */, Properties> taskPropertiesMap;

    public static ConfigService getInstance() {
        return INSTANCE;
    }

    @Override
    public String getConfig(String key) {
        try {
            ConfigKeyConstants.ServerConfiguration serverConfiguration = ConfigKeyConstants.ServerConfiguration.of(key);
            return getServerConfig(serverConfiguration);
        } catch (Throwable ste) {
            try {
                Pair<String, ConfigKeyConstants.TaskConfiguration> taskConfigurationPair = ConfigKeyConstants.TaskConfiguration.getTaskConfigKey(key);
                return getTaskConfig(taskConfigurationPair.getKey(), taskConfigurationPair.getValue());
            } catch (Throwable tte) {
                throw new PtubesRunTimeException(tte);
            }
        }
    }

    // load configs
    @Override public void initialize() {
        LOG.info("FileConfigService start!");

        if (Objects.isNull(serverProperties)) {
            synchronized (this) {
                if (Objects.isNull(serverProperties)) {
                    try {
                        String serverConfig = System.getProperty("ptubes.conf", "classpath:reader.conf");
                        boolean isResourceFile = serverConfig.startsWith(CLASSPATH_URL_PREFIX);
                        String serverConfigPath = (isResourceFile ? StringUtils.substringAfter(serverConfig, CLASSPATH_URL_PREFIX) : serverConfig);
                        String serverConfigParentPath = getServerConfigFileParent(isResourceFile, serverConfigPath);

                        serverProperties = loadProperties(isResourceFile, serverConfigPath);
                        taskPropertiesMap = new HashMap<>();
                        List<String> taskNameList = StringUtil.stringListBreak(getServerConfig(ConfigKeyConstants.ServerConfiguration.TASKS), ",");
                        for (String taskName : taskNameList) {
                            String taskConfigName = taskName + ".properties";
                            Properties taskProperties = loadProperties(false,
                                StringUtils.isBlank(serverConfigParentPath) ? taskConfigName : new File(serverConfigParentPath, taskConfigName).toString());
                            taskPropertiesMap.put(taskName, taskProperties);
                        }
                    } catch (Exception e) {
                        throw new PtubesRunTimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void destroy() {
        this.serverProperties.clear();
        this.serverProperties = null;

        this.taskPropertiesMap.clear();
        this.taskPropertiesMap = null;
        LOG.info("FileConfigService exit!");
    }

    private String getServerConfig(ConfigKeyConstants.ServerConfiguration serverConfiguration) throws InvalidConfigException {
        String res = serverProperties.getProperty(serverConfiguration.getKey(), serverConfiguration.getDefaultValue());
        if (serverConfiguration.isRequired() && StringUtils.isBlank(res)) {
            throw new InvalidConfigException(serverConfiguration.name() + " is required");
        }
        return res;
    }

    private String getTaskConfig(String taskName, ConfigKeyConstants.TaskConfiguration taskConfiguration) throws InvalidConfigException {
        String res = (taskPropertiesMap.containsKey(taskName) ? taskPropertiesMap.get(taskName).getProperty(taskConfiguration.getKey(), taskConfiguration.getDefaultValue()) : null);
        if (taskConfiguration.isRequired() && StringUtils.isBlank(res)) {
            throw new InvalidConfigException(taskConfiguration.name() + " is required");
        }
        return res;
    }

    private Properties loadProperties(boolean isResourceFile, String file) throws FileNotFoundException, IOException {
        return isResourceFile ? loadResourceProperties(file) : loadFileProperties(file);
    }

    private Properties loadFileProperties(String localFile) throws FileNotFoundException, IOException {
        Properties res = new Properties();
        res.load(new FileInputStream(localFile));
        return res;
    }
    private Properties loadResourceProperties(String resourceFile) throws IOException {
        Properties res = new Properties();
        res.load(FileConfigService.class.getClassLoader().getResourceAsStream(resourceFile));
        return res;
    }

    private String getServerConfigFileParent(boolean isResourceFile, String path) {
        if (isResourceFile) {
            File parentFile = new File(FileConfigService.class.getClassLoader().getResource(path).getFile()).getParentFile();
            return Objects.nonNull(parentFile) ? parentFile.toString() : "";
        } else {
            File parentFile = new File(path).getParentFile();
            return Objects.nonNull(parentFile) ? parentFile.toString() : "";
        }
    }
}
