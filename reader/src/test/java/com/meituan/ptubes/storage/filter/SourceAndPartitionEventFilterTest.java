package com.meituan.ptubes.storage.filter;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import com.meituan.ptubes.reader.storage.common.event.EventType;
import com.meituan.ptubes.storage.mock.MockedPtubesEvent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import com.meituan.ptubes.reader.storage.filter.PartitionEventFilter;
import com.meituan.ptubes.reader.storage.filter.SourceAndPartitionEventFilter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class SourceAndPartitionEventFilterTest {
	private static HashMap<String, PartitionEventFilter> sources;
	private static SourceAndPartitionEventFilter eventFilter;

	@BeforeClass
	public static void setUp() throws Exception {
		sources = new HashMap<>();
		sources.put("allAllowedTable", new PartitionEventFilter(true, null, 0));
		sources.put("table1", new PartitionEventFilter(false, new HashSet<>(Arrays.asList(1, 3, 5, 7, 9, 11)), 11));
		sources.put("table2", new PartitionEventFilter(false, new HashSet<>(Arrays.asList(0, 2, 4, 6, 8, 10)), 11));
		eventFilter = new SourceAndPartitionEventFilter(sources);
	}

	@Test
	public void testAllAllowedTable() throws Exception {
		PtubesEvent event = null;

		// allAllowedTable
		event = new MockedPtubesEvent("allAllowedTable", (short) 0, EventType.INSERT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) 0, EventType.DDL);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) 0, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) 0, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("allAllowedTable", (short) -1, EventType.INSERT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) -1, EventType.DDL);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) -1, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) -1, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("allAllowedTable", (short) 2, EventType.INSERT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) 2, EventType.DDL);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) 2, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("allAllowedTable", (short) 2, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));
	}

	@Test
	public void testPartitionFilter() throws Exception {
		PtubesEvent event = null;
		// table1
		event = new MockedPtubesEvent("table1", (short) 2, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 2, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 2, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 2, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table1", (short) 3, EventType.INSERT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 3, EventType.DDL);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 3, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 3, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table1", (short) -1, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) -1, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) -1, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) -1, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table1", (short) 0, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 0, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 0, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table1", (short) 0, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		// table2
		event = new MockedPtubesEvent("table2", (short) 2, EventType.INSERT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 2, EventType.DDL);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 2, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 2, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table2", (short) 3, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 3, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 3, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 3, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table2", (short) -1, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) -1, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) -1, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) -1, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table2", (short) 0, EventType.INSERT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 0, EventType.DDL);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 0, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table2", (short) 0, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));
	}

	@Test
	public void testNotExistTable() throws Exception {
		PtubesEvent event = null;
		// table3, a non-existent table
		event = new MockedPtubesEvent("table3", (short) 2, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 2, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 2, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 2, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table3", (short) 3, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 3, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 3, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 3, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table3", (short) -1, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) -1, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) -1, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) -1, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));

		event = new MockedPtubesEvent("table3", (short) 0, EventType.INSERT);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 0, EventType.DDL);
		Assert.assertFalse(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 0, EventType.COMMIT);
		Assert.assertTrue(eventFilter.allow(event));
		event = new MockedPtubesEvent("table3", (short) 0, EventType.HEARTBEAT);
		Assert.assertTrue(eventFilter.allow(event));
	}
}
