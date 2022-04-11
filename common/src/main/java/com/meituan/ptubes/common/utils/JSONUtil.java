package com.meituan.ptubes.common.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

public class JSONUtil {

	private static final Logger LOG = LoggerFactory.getLogger(JSONUtil.class);

	private static final String JSON_SERIALIZATION_FAILURE_STRING = "JSON serialization failed";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final JsonFactory JSON_FACTORY;

	static {
		//The parser supports parsing single quotes
		OBJECT_MAPPER.configure(Feature.ALLOW_SINGLE_QUOTES, true);
		//The parser supports parsing terminators
		OBJECT_MAPPER.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
		OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		JSON_FACTORY = new JsonFactory(OBJECT_MAPPER);
	}

	/**
	 * Serializes a bean as JSON
	 *
	 * @param <T>
	 * 		the bean type
	 * @param bean
	 * 		the bean to serialize
	 * @param pretty
	 * 		a flag if the output is to be pretty printed
	 * @return the JSON string
	 */
	public static <T> String toJsonString(T bean, boolean pretty)
			throws IOException {

		StringWriter out = new StringWriter(1000);
		JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(out);
		if (pretty) {
			jsonGenerator.useDefaultPrettyPrinter();
		}
		jsonGenerator.writeObject(bean);
		out.flush();

		return out.toString();
	}

	/**
	 * Serializes a bean as JSON. This method will not throw an exception if the serialization fails
	 * but it will instead return the string {@link #JSON_SERIALIZATION_FAILURE_STRING}
	 *
	 * @param <T>
	 * 		the bean type
	 * @param bean
	 * 		the bean to serialize
	 * @param pretty
	 * 		a flag if the output is to be pretty printed
	 * @return the JSON string or {@link #JSON_SERIALIZATION_FAILURE_STRING} if serialization
	 * fails
	 */
	public static <T> String toJsonStringSilent(T bean, boolean pretty) {
		try {
			return toJsonString(bean, pretty);
		} catch (IOException ioe) {
			LOG.error("bean {} to json string error", bean.toString(), ioe);
			return JSON_SERIALIZATION_FAILURE_STRING;
		}
	}

	public static <T> Optional<T> jsonToSimpleBean(String jsonStr, Class<T> clazz) {
		try {
			return Optional.of(OBJECT_MAPPER.readValue(jsonStr, clazz));
		} catch (IOException ioe) {
			LOG.info("parse json {} error", jsonStr, ioe);
			return Optional.empty();
		}
	}

	// search depth = 1
	public static <T> Optional<T> toSimpleColumnBean(String jsonStr, String columnName, Class<T> clazz) {
		try {
			JsonNode rootNode = OBJECT_MAPPER.readTree(jsonStr);
			JsonNode columnNode = rootNode.path(columnName);
			if (columnNode.isMissingNode() == false) {
				return Optional.of(OBJECT_MAPPER.convertValue(columnNode, clazz));
			} else {
				return Optional.empty();
			}
		} catch (IOException ioe) {
			LOG.info("parse json {} error", jsonStr, ioe);
			return Optional.empty();
		}
	}

	public static <T> List<T> jsonToList(String jsonStr, Class<T> clazz) {
		try {
			JavaType type = OBJECT_MAPPER.getTypeFactory().constructParametricType(ArrayList.class, clazz);
			return OBJECT_MAPPER.readValue(jsonStr, type);
		} catch (Throwable t) {
			LOG.error("parse json {} to list error", jsonStr, t);
			return Collections.emptyList();
		}
	}

	public static <K, V> Map<K, V> jsonToMap(String jsonStr, Class<K> kClazz, Class<V> vClazz) {
		try {
			JavaType type = OBJECT_MAPPER.getTypeFactory().constructParametricType(HashMap.class, kClazz, vClazz);
			return OBJECT_MAPPER.readValue(jsonStr, type);
		} catch (Throwable t) {
			LOG.error("parse json {} to map error", jsonStr, t);
			return Collections.emptyMap();
		}
	}

	public static <T> Optional<T> jsonToBean(String jsonStr, TypeReference<T> typeReference) {
		try {
			return Optional.of(OBJECT_MAPPER.readValue(jsonStr, typeReference));
		} catch (IOException ioe) {
			LOG.info("parse json {} error", jsonStr, ioe);
			return Optional.empty();
		}
	}

	/**
	 * Traverse only layers with a depth of 1, output map
	 * @param jsonStr
	 * @return
	 */
	public static Optional<Map<String, String>> jsonToSuperficialMap(String jsonStr) {
		Map<String, String> ans = new LinkedHashMap<>();

		try {
			/*JsonFactory jfactory = new JsonFactory();
			JsonParser jParser = jfactory.createJsonParser(jsonStr); // stream? high performance? try it later */
			JsonNode rootNode = OBJECT_MAPPER.readTree(jsonStr);
			Iterator<Map.Entry<String, JsonNode>> nodesEntry = rootNode.fields();
			while (nodesEntry.hasNext()) {
				Map.Entry<String, JsonNode> node = nodesEntry.next();
				String key = node.getKey();
				JsonNode value = node.getValue();
				if (value.isNull()) {
					ans.put(key, null);
				} else if (value.isContainerNode() || value.isNumber()) {
					ans.put(key, value.toString());
				} else {
					ans.put(key, value.textValue());
				}
			}

			return Optional.of(ans);
		} catch (IOException ioe) {
			LOG.error("json {} to map fail", jsonStr, ioe);
			return Optional.empty();
		}
	}

	/**
	 * just for fun, complex json flattened output
	 * input:
	 * {
	 *     "a":"A",
	 *     "b":{
	 *         "b1":"B1",
	 *         "b2":"B2"
	 *     },
	 *     "c":["C1","C2"],
	 *     "d":[
	 *     	   {
	 *     	       "d1":"D1",
	 *     	       "d2":"D2"
	 *     	   },
	 *     	   {
	 *     	       "dd1":"DD1",
	 *     	       "dd2":"DD2"
	 *     	   }
	 *     ],
	 *     "e":{
	 *         "e1":["ee","eee"],
	 *         "e2":{
	 *             "ee1":"EE1",
	 *             "ee2":"EE2"
	 *         }
	 *     }
	 * }
	 * output:
	 * {a=A, b.b1=B1, b.b2=B2, c[0]=C1, c[1]=C2, d[0].d1=D1, d[0].d2=D2, d[1].dd1=DD1, d[1].dd2=DD2, e.e1[0]=ee, e.e1[1]=eee, e.e2.ee1=EE1, e.e2.ee2=EE2}
	 * @param nodeL0Key
	 * @param nodeL0
	 * @param ans
	 */
	private static void jsonVistor(String nodeL0Key, JsonNode nodeL0, Map<String, String> ans) {
		if (nodeL0.isNull()) {
			ans.put(nodeL0Key, null);
			return;
		}

		if (nodeL0.isValueNode()) {
			ans.put(nodeL0Key, nodeL0.textValue());
			return;
		}

		if (nodeL0.isArray()) {
			Iterator<JsonNode> nodesL1 = nodeL0.iterator();
			int index = 0;
			while (nodesL1.hasNext()) {
				String nodeL1Key = nodeL0Key + "[" + index + "]";
				JsonNode nodeL1 = nodesL1.next();
				if (nodeL1.isValueNode()) {
					ans.put(nodeL1Key, nodeL1.textValue());
				} else {
					jsonVistor(nodeL1Key, nodeL1, ans);
				}
				index++;
			}
		}

		Iterator<Map.Entry<String, JsonNode>> nodesL1 = nodeL0.fields();
		while (nodesL1.hasNext()) {
			Map.Entry<String, JsonNode> nodeL1Entry = nodesL1.next();
			String nodeL1Key = StringUtils.isBlank(nodeL0Key) ? nodeL1Entry.getKey() : nodeL0Key + "." + nodeL1Entry.getKey();
			JsonNode nodeL1 = nodeL1Entry.getValue();

			if (nodeL1.isObject()) {
				Iterator<Map.Entry<String, JsonNode>> nodesL2 = nodeL1.fields();
				while (nodesL2.hasNext()) {
					Map.Entry<String, JsonNode> nodeL2Entry = nodesL2.next();
					String nodeL2Key = nodeL2Entry.getKey();
					JsonNode nodeL2 = nodeL2Entry.getValue();
					jsonVistor(nodeL1Key + "." + nodeL2Key, nodeL2, ans);
				}
			} else {
				jsonVistor(nodeL1Key, nodeL1, ans);
			}
		}
	}
	@Deprecated
	public static Map<String, String> jsonToDeepMap(String jsonStr) {
		if (StringUtils.isBlank(jsonStr)) {
			return Collections.EMPTY_MAP;
		}

		try {
			Map<String, String> ans = new LinkedHashMap<>();
			JsonNode rootNode = OBJECT_MAPPER.readTree(jsonStr);
			jsonVistor("", rootNode, ans);
			return ans;
		} catch (Exception e) {
			LOG.error("parse json {} to Map error", jsonStr, e);
			return Collections.EMPTY_MAP;
		}
	}
}
