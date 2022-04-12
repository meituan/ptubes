package com.meituan.ptubes.container.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.config.producer.TableConfig;
import com.meituan.ptubes.common.utils.JSONUtil;
import com.meituan.ptubes.reader.container.network.request.sub.MySQLSourceSubRequest;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class JsonUtilTest {

	private static final Logger LOG = LoggerFactory.getLogger(JsonUtilTest.class);

	@Test
	public void subRequestJsonParseTest() {
		String jsonStr = "\n"
				+ "{\"partitionClusterInfo\":{\"partitionTotal\":2,\"partitionList\":[0,1]},\"serviceGroupInfo\":{\"taskName\":\"task1\",\"serviceGroupName\":\"serviceGroupName1\",\"databaseInfoSet\":[{\"databaseName\":\"datanbase1\",\"tableNames\":[\"table2\",\"table1\"]}]},\"buffaloCheckpoint\":{\"transactionId\":0,\"eventIndex\":0,\"serverId\":0,\"binlogFile\":0,\"binlogOffset\":0,\"timestamp\":0,\"checkPointMode\":\"LATEST\"}}";

		Optional<MySQLSourceSubRequest> subRequest = JSONUtil.jsonToSimpleBean(jsonStr, MySQLSourceSubRequest.class);
		if (subRequest.isPresent()) {
			LOG.info("subRequest={}", subRequest.get().toString());
		}
	}

	@Test
	public void jsonToDeepMapTest() {
		String jsonStr = "{\n" +
				"    \"a\": \"A\",\n" +
				"    \"b\": {\n" +
				"        \"b1\": \"B1\",\n" +
				"        \"b2\": \"B2\"\n" +
				"    },\n" +
				"    \"c\": [\n" +
				"        \"C1\",\n" +
				"        \"C2\"\n" +
				"    ],\n" +
				"    \"d\": [\n" +
				"        {\n" +
				"            \"d1\": \"D1\",\n" +
				"            \"d2\": \"D2\"\n" +
				"        },\n" +
				"        {\n" +
				"            \"dd1\": \"DD1\",\n" +
				"            \"dd2\": \"DD2\"\n" +
				"        }\n" +
				"    ],\n" +
				"	 \"e\": {\n" +
				"        \"e1\": [\n" +
				"            \"ee\",\n" +
				"            \"eee\"\n" +
				"        ],\n" +
				"        \"e2\": {\n" +
				"            \"ee1\": \"EE1\",\n" +
				"            \"ee2\": \"EE2\"\n" +
				"        }\n" +
				"    }" +
				"}";
		Map<String, String> map = JSONUtil.jsonToDeepMap(jsonStr);
		LOG.debug(map.toString());
	}

	@Test
	public void jsonToSimpleMapTest() {
		String jsonStr = "{\n" +
				"    \"l1\": {\n" +
				"        \"l1_1\": [\n" +
				"            \"l1_1_1\",\n" +
				"            \"l1_1_2\"\n" +
				"        ],\n" +
				"        \"l1_2\": {\n" +
				"            \"l1_2_1\": 121\n" +
				"        }\n" +
				"    },\n" +
				"    \"l2\": {\n" +
				"        \"l2_1\": null,\n" +
				"        \"l2_2\": true,\n" +
				"        \"l2_3\": {}\n" +
				"    }\n" +
				"}";
		String jsonStr2 = "{\n" +
				"    \"a\":null,\n" +
				"    \"b\":\"B\",\n" +
				"    \"c\":100\n" +
				"}";
		Optional<Map<String, String>> map = JSONUtil.jsonToSuperficialMap(jsonStr2);
		for (Map.Entry<String, String> entry : map.get().entrySet()) {
			if (entry.getValue() != null) {
				LOG.debug("{} -> {}", entry.getKey(), entry.getValue());
			}
		}
	}

	@Test
	public void jsonToContainerClassTest() throws Exception {
		Map<String, TableConfig> testCase = new HashMap<String, TableConfig>() {{
			put("a", new TableConfig("database", "table", "id"));
		}};
		String jsonStr = JSONUtil.toJsonString(testCase, true);
		assert StringUtils.isBlank(jsonStr) == false;
		LOG.info(jsonStr);
		Optional<Map<String, TableConfig>> tableConfigMap = JSONUtil.jsonToBean(jsonStr, new TypeReference<Map<String, TableConfig>>() {});
		assert tableConfigMap.isPresent();
		LOG.debug(tableConfigMap.get().toString());
	}

	@Test
	public void bytesToJsonTest() throws Exception {
		List<Byte> origin = new ArrayList<>();
		for (int i = 0; i < 256; i++) {
			origin.add((byte) i);
		}
		byte[] originBytes = new byte[origin.size()];
		for (int i = 0; i < origin.size(); i++) {
			originBytes[i] = origin.get(i);
		}
		System.out.println(bytesToString(originBytes));

		System.out.println(bytesToString(new String(originBytes).getBytes()));

		ByteBuffer byteArray = ByteBuffer.wrap(originBytes);
		Map map = new HashMap() {{
			put("a", StandardCharsets.ISO_8859_1);
		}};
		String jsonStr = JSONUtil.toJsonString(map, true);
		Optional<Map<String, String>> parsedMap = JSONUtil.jsonToSuperficialMap(jsonStr);
		assert parsedMap.isPresent();
		byte[] targetBytes = parsedMap.get().get("a").getBytes();
		System.out.println(bytesToString(targetBytes));
	}

	private String bytesToString(byte[] bytes) {
		List<String> hexStr = new ArrayList<>();
		for (byte target : bytes) {
			hexStr.add(Integer.toHexString(target & 0xFF));
		}
		return Arrays.toString(hexStr.toArray());
	}
}
