package org.uniprot.store.search.domain.impl;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

import org.uniprot.store.search.domain.DatabaseGroup;
import org.uniprot.store.search.domain.Tuple;

@Data
public class DatabaseGroupImpl implements DatabaseGroup {
	private final String groupName;
	private final List<Tuple> items;

	public DatabaseGroupImpl(String groupName, List<Tuple> items) {
		this.groupName = groupName;
		this.items = new ArrayList<>(items);
	}
}
