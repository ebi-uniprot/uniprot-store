package org.uniprot.store.search.domain.impl;

import org.uniprot.store.search.domain.Field;

import lombok.Data;

@Data
public class FieldImpl implements Field {
	private String label;
	private String name;
	public FieldImpl() {
		
	}
	public FieldImpl(String label, String name) {
		this.label = label;
		this.name = name;
	}
}
