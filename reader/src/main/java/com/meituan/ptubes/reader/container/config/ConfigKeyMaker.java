package com.meituan.ptubes.reader.container.config;

import com.meituan.ptubes.reader.container.common.constants.ContainerConstants;

public class ConfigKeyMaker {

	/**
	 * Configuration is located under an appkey
	 * keyName generation rules:
	 * global config:
	 * ptubes-reader.{appkey}.{module}.{property}
	 * task config:
	 * ptubes-reader.{task}.{module}.{property}
	 */

	public final static String PRIVATE_CONFIG_KEY_FORMAT = "ptubes-reader-task.%s.%s.%s";
	public final static String GLOBAL_CONFIG_KEY_FORMAT = "ptubes-reader.%s";

	public enum Module {
		CONTAINER("container"),
		STORAGE("storage"),
		READTASK("readTask"), // theoretically should be producer
		ClIENTSESSION("clientSession"),
		SERVER("server");

		private String moduleName;
		Module(String name) {
			this.moduleName = name;
		}

		public String getModuleName() {
			return moduleName;
		}
	}

	public static String getModuleGlobalKey(String key) {
		return String.format(GLOBAL_CONFIG_KEY_FORMAT, key);
	}

	public static String getModuleKey(String parentId, Module module, String key) {
		// first remove taskName
		return String.format(PRIVATE_CONFIG_KEY_FORMAT, parentId, module.getModuleName(), key);
	}
	public static String getModuleKey(String parentId, String childId, String key) {
		// first remove taskName
		return String.format(PRIVATE_CONFIG_KEY_FORMAT, parentId, childId, key);
	}

	
	

	//=================================== container =====================================

	/**
	 * @return readTask names splitted by ","
	 */
	public static String getContainerReadTaskSetKey() {
		return getModuleGlobalKey(ContainerConstants.CONTAINER_READTASK_SET);
	}

	//=================================== readTask =====================================

	/**
	 * @link ProducerBaseConfig
	 * @param readTaskName
	 * @return
	 */
	public static String getReadTaskBasicConfigKey(String readTaskName) {
		return getModuleKey(readTaskName, Module.READTASK, ContainerConstants.READTASK_BASIC_CONFIG);
	}

	/**
	 * @link RdsConfig
	 * @param readTaskName
	 * @return
	 */
	public static String getReadTaskRDSConfigKey(String readTaskName) {
		return getModuleKey(readTaskName, Module.READTASK, ContainerConstants.READTASK_RDS_CONFIG);
	}

	public static String getReadTaskCdcConfigKey(String readerTaskName) {
		return getModuleKey(readerTaskName, Module.READTASK, ContainerConstants.READTASK_CDC_CONFIG);
	}

	@Deprecated
	public static String getReadTaskCdcExperConfigKey(String readerTaskName) {
		return getModuleKey(readerTaskName, Module.READTASK, ContainerConstants.READTASK_CDC_EXPER_CONFIG);
	}

	/**
	 * @link ProducerConfig#tableConfigs, type=Map<String, TableConfig>
	 * @param readTaskName
	 * @return
	 */
	public static String getReadTaskSubscriptionConfigKey(String readTaskName) {
		return getModuleKey(readTaskName, Module.READTASK, ContainerConstants.READTASK_SUBSCRIPTION_CONFIG);
	}

	public static String getReadTaskSubDatabases(String readTaskName) {
		return getModuleKey(readTaskName, Module.READTASK, ContainerConstants.READTASK_SUBSCRIPTION_DBS);
	}

	public static String getReadTaskSubTables(String readTaskName, String dbName) {
		return getModuleKey(readTaskName, dbName, ContainerConstants.READTASK_SUBSCRIPTION_TABLES);
	}

	//=================================== storage =====================================

	/**
	 * @link StorageConfig
	 * @param readTaskName
	 * @return
	 */
	public static String getStorageBasicConfigKey(String readTaskName) {
		return getModuleKey(readTaskName, Module.STORAGE, ContainerConstants.STORAGE_BASIC_CONFIG);
	}

	/**
	 * @link StorageConfig#FileConfig
	 * @param readTaskName
	 * @return
	 */
	public static String getFileStorageConfigKey(String readTaskName) {
		return getModuleKey(readTaskName, Module.STORAGE, ContainerConstants.STORAGE_FILE_CONFIG);
	}

	/**
	 * @linke StorageConfig#MemConfig
	 * @param readTaskName
	 * @return
	 */
	public static String getMemStorageConfigKey(String readTaskName) {
		return getModuleKey(readTaskName, Module.STORAGE, ContainerConstants.STORAGE_MEM_CONFIG);
	}

	// to be continue...

}
