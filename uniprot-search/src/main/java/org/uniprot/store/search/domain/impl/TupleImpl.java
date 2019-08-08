package org.uniprot.store.search.domain.impl;

import org.uniprot.store.search.domain.Tuple;

import lombok.Data;

@Data
public class TupleImpl implements Tuple {

	private String name;
	private String value;

	public TupleImpl() {

	}

	public TupleImpl(String name, String value) {
		this.name = name;
		this.value = value;
	}

}
