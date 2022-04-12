package com.meituan.ptubes.producer.gtid;

import com.meituan.ptubes.reader.container.common.vo.GtidSet;
import org.junit.Test;


public class GtidTest {
	@Test
	public void gtidSetAddTest() throws Exception {
		GtidSet gtidSet = new GtidSet("09beef2e-6681-11e8-9ebb-246e96a58a48:1-13145646096,c2a745a6-eb3f-11e5-8b89-ecf4bbee4240:1-16553314416,da6e7194-0439-11e9-9193-246e968f3128:1-8267957454,e87cfaec-e554-11e5-a4f5-10517221c4c8:1-1259517492,fe4e8afd-5a70-11e7-a5f1-ec388f6d3b7f:1-3057583921");
		gtidSet.add("da6e7194-0439-11e9-9193-246e968f3128:8267957457");
		System.out.print(gtidSet);
	}
}
