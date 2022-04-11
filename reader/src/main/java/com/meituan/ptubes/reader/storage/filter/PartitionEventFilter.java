package com.meituan.ptubes.reader.storage.filter;

import com.meituan.ptubes.reader.storage.common.event.PtubesEvent;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PartitionEventFilter implements EventFilter {
	private boolean isAllPartitionsWildcard = false;
	private HashSet<Integer> partitionIds = new HashSet<Integer>(8);
	private int partitionCounts;

	public PartitionEventFilter(boolean isAllPartitionsWildcard, HashSet<Integer> partitionIds, int partitionCounts) {
		this.isAllPartitionsWildcard = isAllPartitionsWildcard;
		this.partitionIds = partitionIds;
		this.partitionCounts = partitionCounts;
	}

	@Override
	public boolean allow(PtubesEvent e) {
		return isAllPartitionsWildcard || partitionIds.contains(e.getPartitionId() % partitionCounts);
	}

	public boolean isAllPartitionsWildcard() {
		return isAllPartitionsWildcard;
	}

	public Set<Integer> getPartitionsMask() {
		return Collections.unmodifiableSet(partitionIds);
	}
}
