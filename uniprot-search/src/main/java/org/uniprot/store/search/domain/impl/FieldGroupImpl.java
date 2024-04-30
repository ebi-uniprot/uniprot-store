package org.uniprot.store.search.domain.impl;

import java.util.ArrayList;
import java.util.List;

import org.uniprot.store.search.domain.Field;
import org.uniprot.store.search.domain.FieldGroup;

import lombok.Data;

@Data
public class FieldGroupImpl implements FieldGroup {
    private String groupName;
    private boolean isIsDatabase;
    private List<Field> fields = new ArrayList<>();

    public FieldGroupImpl() {}

    public FieldGroupImpl(String groupName, List<Field> fields) {
        this(groupName, false, fields);
    }

    public FieldGroupImpl(String groupName, boolean isDatabase, List<Field> fields) {
        super();
        this.groupName = groupName;
        this.isIsDatabase = isDatabase;
        this.fields = new ArrayList<>(fields);
        ;
    }
}
