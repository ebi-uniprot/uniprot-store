package org.uniprot.store.search.domain.impl;

import lombok.Data;

import org.uniprot.store.search.domain.Field;

@Data
public class FieldImpl implements Field {
    private String label;
    private String name;
    private String javaFieldName;

    public FieldImpl() {}

    public FieldImpl(String label, String name) {
        this(label, name, name);
    }

    public FieldImpl(String label, String name, String javaFieldName) {
        this.label = label;
        this.name = name;
        this.javaFieldName = javaFieldName;
    }

    @Override
    public boolean hasReturnField(String fieldName) {
        return true;
    }
}
