package com.meituan.ptubes.container.avro;

import com.google.common.collect.Lists;
import com.meituan.ptubes.reader.container.common.config.service.ProducerConfigService;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.LongColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.NullColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Pair;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Row;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.TimestampColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.Column;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.RecordUtil;
import com.meituan.ptubes.reader.schema.service.ISchemaService;
import com.meituan.ptubes.reader.storage.common.event.PtubesEventV2;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import java.io.File;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerBaseConfig;
import com.meituan.ptubes.reader.container.common.config.producer.ProducerConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsConfig;
import com.meituan.ptubes.reader.container.common.config.producer.RdsUserPassword;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.reader.container.common.vo.KeyPair;
import com.meituan.ptubes.reader.container.common.vo.MySQLBinlogInfo;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.VersionedSchema;
import com.meituan.ptubes.reader.schema.provider.ReadTaskSchemaProvider;
import com.meituan.ptubes.reader.schema.provider.WriteTaskSchemaProvider;
import com.meituan.ptubes.reader.schema.service.FileBasedISchemaService;
import com.meituan.ptubes.reader.storage.common.event.MySQLChangeEntry;
import com.meituan.ptubes.sdk.protocol.RdsPacket;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AvroEventDecoderTest {
	private String processWorkingPath;
	private String schemaPath;

	private ISchemaService schemaService;
	private Map<String, TableConfig> tableConfigMap;
	private RdsConfig rdsConfig;
	private ReadTaskSchemaProvider readTaskSchemaProvider;
	private WriteTaskSchemaProvider writeTaskSchemaProvider;

	@Before
	public void prepareAvroDecoder() throws Exception {
		this.processWorkingPath = new File("").getAbsoluteFile().getAbsolutePath(); // in the schema directory
		if(StringUtils.contains(processWorkingPath,"/target/test-classes/")){
			processWorkingPath = processWorkingPath.substring(0, processWorkingPath.indexOf("/target/test-classes/"));
		}
		System.out.println(this.processWorkingPath);
		this.schemaPath = this.processWorkingPath + File.separator + "src/test/java/com/meituan/ptubes/container/dts";
		System.out.println(this.schemaPath);

		this.schemaService = new FileBasedISchemaService(schemaPath);
		this.tableConfigMap = new HashMap<String, TableConfig>() {{
			put("testdb.tn", new TableConfig("testdb", "tn", "id"));
		}};
		this.schemaService = new FileBasedISchemaService(schemaPath);
		RdsUserPassword rdsUserPassword = new RdsUserPassword(
			"xxx",
			"******"
		);
		ProducerConfigService producerConfigService = new ProducerConfigService(new ProducerConfig(
			"testReadTask",
			new ProducerBaseConfig(),
			tableConfigMap,
			rdsConfig
		));
		this.readTaskSchemaProvider = new ReadTaskSchemaProvider(
			"testReadTask",
			rdsConfig,
			rdsUserPassword,
			schemaService,
			producerConfigService
		);
		this.writeTaskSchemaProvider = new WriteTaskSchemaProvider("testReadTask", schemaService);

		readTaskSchemaProvider.start();
		writeTaskSchemaProvider.start();
	}

	@After
	public void destroyAvroDecoder() {
//		decoder.close();
		readTaskSchemaProvider.stop();
		writeTaskSchemaProvider.stop();
	}

	@Test
	public void decodePBEvent() throws Exception {
		String fullTableName = "testdb.tn";
		String comment = "";
		final long fromIP = 0xFF000001;
		VersionedSchema versionedSchema = readTaskSchemaProvider.loadTableSchema(
			new SchemaVersion(
				fullTableName,
				1),
			0);
		// mock Row
		Pair<Row> rowPair = new Pair<>();
		rowPair.setBefore(new Row(Lists.newArrayList(new Column[]{
			LongColumn.valueOf(100), NullColumn.valueOf(1), StringColumn.valueOf("efg".getBytes()),
			TimestampColumn.valueOf(new Timestamp(System.currentTimeMillis())),})));
		rowPair.setAfter(new Row(Lists.newArrayList(new Column[]{LongColumn.valueOf(100), /*NullColumn.valueOf(1)*/
			StringColumn.valueOf("ABC".getBytes()), //Column number?
			StringColumn.valueOf("EFG".getBytes()),
			TimestampColumn.valueOf(new Timestamp(System.currentTimeMillis())),})));
		// mock PB Event
		MySQLBinlogInfo binlogInfo = new MySQLBinlogInfo(
			(short) 1,
			123456,
			1,
			1024,
			"xxxxxx-xxxxxx".getBytes(),
			123,
			0,
			System.currentTimeMillis());
		List<KeyPair> kps = new ArrayList<>();
		RdsPacket.RdsEvent pbEvent = RecordUtil.generatePtubesEvent(
			"UnitTest_ReaderTask",
			kps,
			binlogInfo,
			versionedSchema,
			EventType.UPDATE,
			fullTableName,
			rowPair,
			System.currentTimeMillis(),
			System.currentTimeMillis(),
			comment,
			"",
			fromIP);
		// mock dbChangeEntry
		MySQLChangeEntry dbEntry = new MySQLChangeEntry(
			fullTableName,
			0xFF000001,
			binlogInfo,
			System.nanoTime(),
			System.nanoTime(),
			null,
			EventType.UPDATE,
//			versionedSchema.getSchema(),
//			versionedSchema.getVersion().getVersion(),
			kps,
			pbEvent);
		// get encodedEvent
		byte[] encodedEvent = dbEntry.getSerializedChangeEntry();
		PtubesEventV2 ptubesEvent = new PtubesEventV2(
			ByteBuffer.wrap(encodedEvent),
			0);
		// decoding
		RdsPacket.RdsEvent decodedEvent = RdsPacket.RdsEvent.parseFrom(ptubesEvent.getPayload());
		assert decodedEvent.equals(pbEvent);
	}

}
