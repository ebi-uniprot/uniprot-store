package org.uniprot.store.config.returnfield.config;

import java.util.List;

import org.uniprot.store.config.returnfield.model.ReturnField;

public interface ReturnFieldConfig {
    List<ReturnField> getAllFields();

    List<ReturnField> getReturnFields();

    ReturnField getReturnFieldByName(String fieldName);

    boolean returnFieldExists(String fieldName);
}
