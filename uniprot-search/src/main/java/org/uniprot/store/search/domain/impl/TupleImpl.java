package org.uniprot.store.search.domain.impl;

import lombok.Data;

import org.uniprot.store.search.domain.Tuple;

@Data
public class TupleImpl implements Tuple {

    private String name;
    private String value;

    public TupleImpl() {}

    public TupleImpl(String name, String value) {
        this.name = name;
        this.value = value;
    }
}
