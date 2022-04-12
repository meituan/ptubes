package com.meituan.ptubes.container.service;

import com.meituan.ptubes.reader.schema.service.ISchemaService;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.Arrays;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.service.FileBasedISchemaService;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileBasedSchemaServiceTest {

	private static final Logger LOG = LoggerFactory.getLogger("test");

	private ISchemaService schemaService;
	private String resouresPath;
	private String processWorkingPath;
	private String schemaPath;

	@Before
	public void prepareSchemaService() throws Exception {
		this.resouresPath = FileBasedSchemaServiceTest.class.getClassLoader().getResource("").getPath();
		this.processWorkingPath = new File("").getAbsolutePath();
		if(StringUtils.contains(processWorkingPath,"/target/test-classes/")){
			processWorkingPath = processWorkingPath.substring(0, processWorkingPath.indexOf("/target/test-classes/"));
		}
		LOG.info("test resources path:{}", resouresPath);
		LOG.info("work space path: {}", processWorkingPath);

		this.schemaPath = processWorkingPath + File.separator + "src/test/java/com/meituan/ptubes/container/dts";
		this.schemaService = new FileBasedISchemaService(schemaPath);
	}

	@After
	public void destroySchemaService() {
		// do nothing
	}

	@Test
	public void dirIsReadableTest() throws Exception {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(processWorkingPath, "src/test/java/com/meituan/ptubes/container/dts/ta.1.dtsc")));
		int availableBytes = 0;
		while ((availableBytes = bis.available()) > 0) {
			byte[] buf = new byte[availableBytes];
			bis.read(buf, 0, availableBytes);
			LOG.debug("read test file: {}", new String(buf));
		}
	}

	@Test
	public void loadOneTableLatestSchemaTest() throws Exception {
		VersionedSchema versionedSchema = schemaService.loadLatestTableSchema("ta");
		assert versionedSchema != null;
		assert versionedSchema.getVersion().equals(new SchemaVersion("ta", 3));
		assert versionedSchema.getTableMeta().getOrderedFieldMetaList().size() == 3;
		LOG.debug(versionedSchema.toString());
	}

	@Test
	public void loadAllTablesLatestSchemaTest() throws Exception {
		Map<String, VersionedSchema> versionedSchemaMap = schemaService.fetchLatestTableSchema(new HashSet<String>(Arrays.asList(new String[]{"ta", "tb"})));
		for (Map.Entry<String, VersionedSchema> entry : versionedSchemaMap.entrySet()) {
			String table = entry.getKey();
			VersionedSchema versionedSchema = entry.getValue();

			assert table.equals("tc") == false; // don't load tc table
			switch (table) {
			case "ta":
				assert versionedSchema != null;
				assert versionedSchema.getVersion().equals(new SchemaVersion("ta", 3));
				assert versionedSchema.getTableMeta().getOrderedFieldMetaList().size() == 3;
				break;
			case "tb":
				assert versionedSchema != null;
				assert versionedSchema.getVersion().equals(new SchemaVersion("tb", 10));
				assert versionedSchema.getTableMeta().getOrderedFieldMetaList().size() == 2;
				break;
			}
			LOG.debug(versionedSchema.toString());
		}
	}

	@Test
	public void loadRefTableVersionSchemaTest() throws Exception {
		VersionedSchema versionedSchema = schemaService.loadTableSchemaByVersion(new SchemaVersion("ta", 2));
		assert versionedSchema != null;
		assert versionedSchema.getVersion().equals(new SchemaVersion("ta", 2));
		assert versionedSchema.getTableMeta().getOrderedFieldMetaList().size() == 4;
		LOG.debug(versionedSchema.toString());
	}

	@Test
	public void loadAllTableSchemaTest() throws Exception {
		Map<String, TreeMap<SchemaVersion, VersionedSchema>> schemaMap = schemaService.fetchAllTableSchemas(
				new HashSet<String>(Arrays.asList(new String[] { "ta", "tb" })));
		for (Map.Entry<String, TreeMap<SchemaVersion, VersionedSchema>> schemasEntry : schemaMap.entrySet()) {
			String table = schemasEntry.getKey();
			TreeMap<SchemaVersion, VersionedSchema> map = schemasEntry.getValue();
			NavigableSet<SchemaVersion> schemaKeys = map.navigableKeySet();
			Iterator<SchemaVersion> iterator = schemaKeys.iterator();
			assert table.equals("tc") == false; // don't load tc table
			switch(table) {
			case "ta":
				assert map.size() == 3;
				break;
			case "tb":
				assert map.size() == 1;
				break;
			}
			while (iterator.hasNext()) {
				SchemaVersion schemaVersion = iterator.next();
				VersionedSchema versionedSchema = map.get(schemaVersion);
				assert versionedSchema != null;
				LOG.debug(versionedSchema.toString());
			}
		}
	}

	@Test
	public void loadAllTableSchemaRefNumTest() throws Exception {
		Map<String, TreeMap<SchemaVersion, VersionedSchema>> schemaMap = schemaService.fetchPartialTableSchemas(
				new HashSet<String>(Arrays.asList(new String[] { "ta", "tb" })), 2);
		for (Map.Entry<String, TreeMap<SchemaVersion, VersionedSchema>> schemasEntry : schemaMap.entrySet()) {
			String table = schemasEntry.getKey();
			TreeMap<SchemaVersion, VersionedSchema> map = schemasEntry.getValue();
			NavigableSet<SchemaVersion> schemaKeys = map.navigableKeySet();
			Iterator<SchemaVersion> iterator = schemaKeys.iterator();
			assert table.equals("tc") == false; // don't load tc table
			switch(table) {
			case "ta":
				assert map.size() == 2;
				break;
			case "tb":
				assert map.size() == 1;
				break;
			}
			while (iterator.hasNext()) {
				SchemaVersion schemaVersion = iterator.next();
				VersionedSchema versionedSchema = map.get(schemaVersion);
				assert versionedSchema != null;
				if (table.equals("ta")) {
					assert schemaVersion.getVersion() != 1; // ta.1.avsc is not loaded
				}
				LOG.debug(versionedSchema.toString());
			}
		}
	}

}
