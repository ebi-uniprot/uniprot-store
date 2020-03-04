package org.uniprot.store.config.returnfield.common;

import org.uniprot.store.config.returnfield.model.ReturnField;

import java.util.List;

public interface ReturnFieldConfig {
    List<ReturnField> getReturnFields();

    ReturnField getReturnFieldByName(String fieldName);

    boolean returnFieldExists(String fieldName);
}
