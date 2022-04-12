package com.meituan.ptubes.common.utils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;

public class JSONUtilTest {

    public static class TestClass {
        private String a;
        private int b;

        public TestClass() {
        }

        public TestClass(String a, int b) {
            this.a = a;
            this.b = b;
        }

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public int getB() {
            return b;
        }

        public void setB(int b) {
            this.b = b;
        }
    }

    @Test
    public void toListTest() {
        final String tc = "[{\"a\": \"isA\", \"b\": 1},{\"a\": \"isB\", \"b\": 2}]";

        List<TestClass> res = JSONUtil.jsonToList(tc, TestClass.class);
        assert CollectionUtils.isNotEmpty(res) && res.size() == 2;
        assert res.get(1).getA().equals("isB") && res.get(1).getB() == 2;
    }

    @Test
    public void toSimpleColumnBean() {
        final String tc = "{\"objA\": {\"a\":\"isA\", \"b\":3}, \"objB\": {\"a\":\"isB\", \"b\":-1}}";
        Optional<TestClass> a = JSONUtil.toSimpleColumnBean(tc, "objB", TestClass.class);
        assert a.isPresent() && "isB".equals(a.get().getA());
    }

    @Test
    public void toMapTest() {
//        Map<String, TestClass> map = new HashMap<>();
//        map.put("abc", new TestClass("abc", 1));
//        map.put("def", new TestClass("def", 2));
//
//        System.out.println(JSONUtil.toJsonStringSilent(map, false));

        final String tc = "{\"abc\":{\"a\":\"abc\",\"b\":1},\"def\":{\"a\":\"def\",\"b\":2}}";

        Map<String, TestClass> res = JSONUtil.jsonToMap(tc, String.class, TestClass.class);
        assert MapUtils.isNotEmpty(res) && res.size() == 2;
        assert res.containsKey("abc") && res.get("abc").getA().equals("abc") && res.get("def").getB() == 2;
    }

}
