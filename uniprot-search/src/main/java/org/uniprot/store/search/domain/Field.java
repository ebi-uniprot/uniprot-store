package org.uniprot.store.search.domain;

import org.uniprot.store.search.field.ReturnField;

public interface Field extends ReturnField {
    String getLabel();

    String getName();
}
